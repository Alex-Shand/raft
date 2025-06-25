use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use macros::spawn;
use tokio::{
    signal,
    sync::oneshot::{Receiver, Sender},
    time::Instant,
};

use crate::{
    datastore::DataStore,
    network::NetworkHandle,
    rpc::{self, RpcClient, RpcServer},
    util::{Locked, Race as _},
};

#[derive(Debug, Copy, Clone)]
enum Role {
    Leader,
    Candidate,
    Follower,
    Dead,
}

#[derive(Debug, Clone)]
pub(crate) enum Leadership {
    Me,
    Leader(String),
    Unknown,
}

#[derive(Debug, Clone)]
enum LogCommand {
    Null,
    Get(String),
    Set(String, String),
    Delete(String),
}

#[derive(Debug, Clone)]
pub(crate) struct LogEntry {
    command: LogCommand,
    term: usize,
}

pub(crate) struct Node {
    pub(crate) name: String,
    peers: HashSet<String>,
    rpc: RpcClient,
    logs_ready: tokio::sync::mpsc::Sender<()>,
    role: Locked<Role>,
    term: Locked<usize>,
    last_heartbeat: Locked<Instant>,
    last_vote: Locked<Option<String>>,
    current_leader: Locked<Option<String>>,
    thrash: Locked<bool>,
    store: Locked<DataStore>,
    log: Locked<Vec<LogEntry>>,
    next_index: Locked<HashMap<String, usize>>,
    match_index: Locked<HashMap<String, usize>>,
    commit_index: Locked<usize>,
    last_applied: Locked<usize>,
    subscriptions1: Locked<HashMap<usize, Sender<Option<String>>>>,
    subscriptions2: Locked<HashMap<usize, Sender<()>>>,
}

#[derive(Debug)]
#[expect(dead_code)]
pub(crate) struct NodeStatus {
    role: Role,
    leader: Leadership,
    term: usize,
    store: DataStore,
}

impl Node {
    pub(crate) async fn new(
        name: impl Into<String>,
        peers: impl Iterator<Item = impl Into<String>>,
        network: NetworkHandle,
        store: DataStore,
        thrash: bool,
    ) -> Arc<Self> {
        let name = name.into();
        let mut peers = peers.map(Into::into).collect::<HashSet<_>>();
        let _ = peers.remove(&name);
        let (sender, reciever) = tokio::sync::mpsc::channel(1);
        let this = Arc::new(Self {
            name,
            peers,
            logs_ready: sender,
            rpc: RpcClient::new(network.clone()),
            role: Locked::new(Role::Dead),
            term: Locked::new(0),
            last_heartbeat: Locked::new(Instant::now()),
            last_vote: Locked::new(None),
            current_leader: Locked::new(None),
            thrash: Locked::new(thrash),
            store: Locked::new(store),
            log: Locked::new(vec![LogEntry {
                command: LogCommand::Null,
                term: 0,
            }]),
            next_index: Locked::new(HashMap::new()),
            match_index: Locked::new(HashMap::new()),
            commit_index: Locked::new(0),
            last_applied: Locked::new(0),
            subscriptions1: Locked::new(HashMap::new()),
            subscriptions2: Locked::new(HashMap::new()),
        });
        rpc::start_rpc(network, this.clone()).await;
        this.clone().start_submitter(reciever).await;
        this
    }

    #[spawn]
    async fn start_submitter(
        self: Arc<Self>,
        signal: tokio::sync::mpsc::Receiver<()>,
    ) -> ! {
        let mut signal = signal;
        loop {
            let Some(()) = signal.recv().await else {
                continue;
            };
            let commit_index = self.commit_index.get().await;
            let mut last_applied = self.last_applied.write().await;
            let entries = if commit_index > *last_applied {
                let entries = self.log.read().await
                    [*last_applied + 1..=commit_index]
                    .to_vec();
                *last_applied = commit_index;
                entries
            } else {
                Vec::new()
            };
            drop(last_applied);
            for (id, entry) in entries.into_iter().enumerate() {
                let id = commit_index + id;
                match entry.command {
                    LogCommand::Null => (),
                    LogCommand::Get(key) => {
                        let result = self.store.read().await.get(&key);
                        if let Some(sender) =
                            self.subscriptions1.write().await.remove(&id)
                        {
                            let _ = sender.send(result);
                        }
                    }
                    LogCommand::Set(key, value) => {
                        self.store.write().await.set(key, value);
                        if let Some(sender) =
                            self.subscriptions2.write().await.remove(&id)
                        {
                            let _ = sender.send(());
                        }
                    }
                    LogCommand::Delete(key) => {
                        self.store.write().await.delete(&key);
                        if let Some(sender) =
                            self.subscriptions2.write().await.remove(&id)
                        {
                            let _ = sender.send(());
                        }
                    }
                }
            }
        }
    }

    pub(crate) async fn status(&self) -> NodeStatus {
        NodeStatus {
            role: self.role.get().await,
            leader: self.get_leader().await,
            term: self.term.get().await,
            store: self.store.read().await.clone(),
        }
    }

    pub(crate) async fn start(self: Arc<Self>) {
        self.start_follower(0).await;
    }

    pub(crate) async fn kill(&self) {
        self.role.set(Role::Dead).await;
    }

    pub(crate) async fn set_thrash(&self, thrash: bool) {
        self.thrash.set(thrash).await;
    }

    pub(crate) async fn get_leader(&self) -> Leadership {
        match self.role.get().await {
            Role::Leader => Leadership::Me,
            Role::Candidate | Role::Dead => Leadership::Unknown,
            Role::Follower => self
                .current_leader
                .read()
                .await
                .as_ref()
                .cloned()
                .map_or(Leadership::Unknown, Leadership::Leader),
        }
    }

    pub(crate) async fn get(
        &self,
        key: String,
    ) -> Option<Receiver<Option<String>>> {
        let (sender, reciever) = tokio::sync::oneshot::channel();
        let id = self.push_log(LogCommand::Get(key)).await?;
        let _ = self.subscriptions1.write().await.insert(id, sender);
        Some(reciever)
    }

    pub(crate) async fn set(
        &self,
        key: String,
        value: String,
    ) -> Option<Receiver<()>> {
        let (sender, reciever) = tokio::sync::oneshot::channel();
        let id = self.push_log(LogCommand::Set(key, value)).await?;
        let _ = self.subscriptions2.write().await.insert(id, sender);
        Some(reciever)
    }

    pub(crate) async fn delete(&self, key: String) -> Option<Receiver<()>> {
        let (sender, reciever) = tokio::sync::oneshot::channel();
        let id = self.push_log(LogCommand::Delete(key)).await?;
        let _ = self.subscriptions2.write().await.insert(id, sender);
        Some(reciever)
    }

    async fn push_log(&self, command: LogCommand) -> Option<usize> {
        if let Role::Leader = self.role.get().await {
            let term = self.term.get().await;
            let mut log = self.log.write().await;
            log.push(LogEntry { command, term });
            return Some(log.len() - 1);
        }
        None
    }

    async fn start_follower(self: Arc<Self>, term: usize) {
        self.role.set(Role::Follower).await;
        self.term.set(term).await;
        self.last_vote.set(None).await;
        self.current_leader.set(None).await;
        self.election_timer().await;
    }

    #[spawn]
    async fn election_timer(self: Arc<Self>) {
        let start_term = self.term.get().await;
        let timeout = if self.thrash.get().await {
            Duration::from_millis(150)
        } else {
            Duration::from_millis(rand::random_range(150..=300))
        };

        // Wake up every 10 miliseconds to check if we need to stop waiting
        loop {
            tokio::time::sleep(Duration::from_millis(10)).await;

            // We don't need to run elections if we're the leader or we're dead
            if matches!(self.role.get().await, Role::Leader | Role::Dead) {
                return;
            }

            // If the term has advanced cancel this task, if we're still a
            // follower in the new term there will be another one running
            if self.term.get().await != start_term {
                return;
            }

            // Launch an election if the timeout expires
            if self.last_heartbeat.get().await.elapsed() >= timeout {
                self.start_election().await;
                return;
            }
        }
    }

    async fn start_election(self: Arc<Self>) {
        // This node becomes a candidate in a new term and votes for itself
        self.role.set(Role::Candidate).await;
        let election_term = self.term.incr().await;
        self.last_vote.set(Some(self.name.clone())).await;
        self.current_leader.set(None).await;
        let (last_log_index, last_log_term) =
            self.get_last_log_index_and_term().await;

        // Start with one vote because the node voted for itself
        let total_votes = Arc::new(Locked::new(1));

        // Start a new election timer, if we manage to get enough votes this
        // will terminate itself once we become leader
        self.clone().election_timer().await;

        // Ask all the peers for votes in parallel. We can assume leadership as
        // soon as enough votes come back even if some are outstanding
        self.peers
            .iter()
            .map(|peer| {
                self.clone().request_vote_from(
                    peer.to_owned(),
                    election_term,
                    last_log_index,
                    last_log_term,
                    total_votes.clone(),
                )
            })
            .race()
            .await;
    }

    async fn get_last_log_index_and_term(&self) -> (usize, usize) {
        let log = self.log.read().await;
        let last_log_index = log.len() - 1;
        (last_log_index, log[last_log_index].term)
    }

    async fn request_vote_from(
        self: Arc<Self>,
        peer: String,
        election_term: usize,
        last_log_index: usize,
        last_log_term: usize,
        total_votes: Arc<Locked<usize>>,
    ) {
        // Ask for a vote, this could block forever or return nothing at the
        // whims of the network
        let Some((response_term, vote_granted)) = self
            .rpc
            .to(&peer)
            .request_vote(
                &self.name,
                election_term,
                last_log_index,
                last_log_term,
            )
            .await
        else {
            // Bail if we know there is no response coming
            return;
        };

        // If we've stopped being a candidate while waiting for the response
        // bail
        let Role::Candidate = self.role.get().await else {
            return;
        };

        // The response came back with a higher term, we've fallen behind so
        // become a follower in the new term
        if response_term > election_term {
            self.start_follower(response_term).await;
            return;
        }

        // Got a vote
        if response_term == election_term && vote_granted {
            let votes = total_votes.incr().await;
            // Won the election, become the leader
            if votes * 2 > self.peers.len() + 1 {
                self.start_leader().await;
            }
        }
    }

    async fn start_leader(self: Arc<Self>) {
        self.role.set(Role::Leader).await;
        let ni = self.log.read().await.len();
        {
            let mut next_index = self.next_index.write().await;
            let mut match_index = self.match_index.write().await;
            for peer in &self.peers {
                let _ = next_index.insert(peer.to_owned(), ni);
                let _ = match_index.insert(peer.to_owned(), 0);
            }
        }
        // The leader sends out heartbeats immedatly upon assuming leadership,
        // then again every 50ms.
        loop {
            self.clone().send_heartbeat().await;
            tokio::time::sleep(Duration::from_millis(50)).await;
            if !matches!(self.role.get().await, Role::Leader) {
                // Lost leadership, something else will have started the
                // follower loop so we can just exit this one
                return;
            }
        }
    }

    async fn send_heartbeat(self: Arc<Self>) {
        // Only leaders send heartbeats (this might be overkill since
        // start_leader checks too)
        if !matches!(self.role.get().await, Role::Leader) {
            return;
        }

        // Send the heartbeat message to all peers in parallel
        let current_term = self.term.get().await;
        self.peers
            .iter()
            .map(|peer| {
                self.clone()
                    .send_heartbeat_to(peer.to_owned(), current_term)
            })
            .race()
            .await;
    }

    async fn send_heartbeat_to(
        self: Arc<Self>,
        peer: String,
        current_term: usize,
    ) {
        let log = self.log.read().await;
        let ni = self.next_index.read().await[&peer];
        let prev_index = ni - 1;
        let prev_term = log[prev_index].term;
        let entries = &log[ni..];

        // Same as request_vote rpc
        let Some((response_term, success)) = self
            .rpc
            .to(&peer)
            .append_logs(
                current_term,
                &self.name,
                prev_index,
                prev_term,
                entries,
                self.commit_index.get().await,
            )
            .await
        else {
            return;
        };

        // The heartbeat response contains a higher term, we've fallen behind,
        // become a follower
        if response_term > current_term {
            self.clone().start_follower(response_term).await;
        }

        if matches!(self.role.get().await, Role::Leader)
            && current_term == response_term
        {
            if success {
                let _ = self
                    .next_index
                    .write()
                    .await
                    .insert(peer.clone(), ni + entries.len());
                let _ = self
                    .match_index
                    .write()
                    .await
                    .insert(peer.clone(), ni + entries.len() - 1);
                self.check_for_committed_entries(
                    current_term,
                    &self.peers,
                    &log,
                )
                .await;
            } else {
                let _ =
                    self.next_index.write().await.insert(peer.clone(), ni - 1);
            }
        }
    }

    async fn check_for_committed_entries(
        &self,
        current_term: usize,
        peers: &HashSet<String>,
        log: &[LogEntry],
    ) {
        let commit_index = self.commit_index.get().await;
        for (index, entry) in log.iter().enumerate().skip(commit_index + 1) {
            if entry.term == current_term {
                let mut match_count = 1;
                for peer in peers {
                    if self.match_index.read().await[peer] >= index {
                        match_count += 1;
                    }
                }
                if match_count * 2 > peers.len() + 1 {
                    self.commit_index.set(index).await;
                }
            }
        }
        if self.commit_index.get().await != commit_index {
            let _ = self.logs_ready.try_send(());
        }
    }

    async fn can_vote_for(
        &self,
        node: &str,
        requestor_last_log_index: usize,
        requestor_last_log_term: usize,
    ) -> bool {
        let (last_log_index, last_log_term) =
            self.get_last_log_index_and_term().await;
        let last_log_compatible = requestor_last_log_term > last_log_term
            || (requestor_last_log_term == last_log_term
                && requestor_last_log_index >= last_log_index);
        let Some(vote) = &*self.last_vote.read().await else {
            return last_log_compatible;
        };
        vote == node && last_log_compatible
    }

    async fn common_rpc_checks(self: Arc<Self>, term: usize) -> Option<()> {
        // Dead nodes don't respond
        if matches!(self.role.get().await, Role::Dead) {
            return None;
        }

        // If we get a message that contains a higher term make sure we're a
        // follower
        //
        // This kicks off the follower loop in a new task because election_timer
        // is #[spawn]
        //
        // If this node was already a follower or candidate in it's current term
        // *that* election timer will detect the term change and exit
        //
        // If this node was a leader in the current term the leader loop will
        // detect the role change and exit
        if term > self.term.get().await {
            self.clone().start_follower(term).await;
        }

        Some(())
    }
}

#[async_trait::async_trait]
impl RpcServer for Arc<Node> {
    async fn request_vote(
        &self,
        node: String,
        term: usize,
        last_log_index: usize,
        last_log_term: usize,
    ) -> Option<(usize, bool)> {
        self.clone().common_rpc_checks(term).await?;

        // If we can vote for the requesting node, record our vote, reset the
        // election timeout and tell the node
        if self.term.get().await == term
            && self
                .can_vote_for(&node, last_log_index, last_log_term)
                .await
        {
            self.last_vote.set(Some(node)).await;
            self.last_heartbeat.set(Instant::now()).await;
            return Some((term, true));
        }

        // Otherwise tell it we can't vote
        Some((term, false))
    }

    async fn append_logs(
        &self,
        term: usize,
        leader: String,
        prev_index: usize,
        prev_term: usize,
        entries: Vec<LogEntry>,
        leader_commit: usize,
    ) -> Option<(usize, bool)> {
        self.clone().common_rpc_checks(term).await?;

        // If the term matches we can accept the heartbeat
        if self.term.get().await == term {
            match self.role.get().await {
                Role::Follower => {
                    self.current_leader.set(Some(leader)).await;
                }
                // If we've somehow died while trying to get the term lock, bail
                Role::Dead => return None,
                // If we're recieving heartbeats, clearly somebody else thinks
                // they're the leader, so become a follower
                _ => self.clone().start_follower(term).await,
            }
            // Reset the election timer
            self.last_heartbeat.set(Instant::now()).await;

            let mut log = self.log.write().await;
            if prev_index < log.len() && prev_term == log[prev_index].term {
                let mut insert_index = prev_index + 1;
                let mut new_entries_index = 0;
                loop {
                    if insert_index >= log.len()
                        || new_entries_index != entries.len()
                    {
                        break;
                    }
                    if log[insert_index].term != entries[new_entries_index].term
                    {
                        break;
                    }
                    insert_index += 1;
                    new_entries_index += 1;
                }

                if new_entries_index < entries.len() {
                    drop(log.drain(insert_index..));
                    log.extend(entries[new_entries_index..].iter().cloned());
                }

                let commit_index = self.commit_index.get().await;
                if leader_commit > commit_index {
                    self.commit_index
                        .set(usize::min(leader_commit, log.len()))
                        .await;
                    let _ = self.logs_ready.try_send(());
                }

                return Some((term, true));
            }
        }

        Some((term, false))
    }
}
