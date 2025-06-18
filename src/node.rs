use std::{collections::HashSet, sync::Arc, time::Duration};

use macros::spawn;
use tokio::time::Instant;

use crate::{
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

pub(crate) struct Node {
    pub(crate) name: String,
    peers: HashSet<String>,
    rpc: RpcClient,
    role: Locked<Role>,
    term: Locked<usize>,
    last_heartbeat: Locked<Instant>,
    last_vote: Locked<Option<String>>,
    thrash: Locked<bool>,
}

#[derive(Debug)]
#[expect(dead_code)]
pub(crate) struct NodeStatus {
    role: Role,
    term: usize,
}

impl Node {
    pub(crate) async fn new(
        name: impl Into<String>,
        peers: impl Iterator<Item = impl Into<String>>,
        network: NetworkHandle,
        thrash: bool,
    ) -> Arc<Self> {
        let name = name.into();
        let mut peers = peers.map(Into::into).collect::<HashSet<_>>();
        let _ = peers.remove(&name);
        let this = Arc::new(Self {
            name,
            peers,
            rpc: RpcClient::new(network.clone()),
            role: Locked::new(Role::Dead),
            term: Locked::new(0),
            last_heartbeat: Locked::new(Instant::now()),
            last_vote: Locked::new(None),
            thrash: Locked::new(thrash),
        });
        rpc::start_rpc(network, this.clone()).await;
        this
    }

    pub(crate) async fn status(&self) -> NodeStatus {
        NodeStatus {
            role: self.role.get().await,
            term: self.term.get().await,
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

    async fn start_follower(self: Arc<Self>, term: usize) {
        self.role.set(Role::Follower).await;
        self.term.set(term).await;
        self.last_vote.set(None).await;
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
                    total_votes.clone(),
                )
            })
            .race()
            .await;
    }

    async fn request_vote_from(
        self: Arc<Self>,
        peer: String,
        election_term: usize,
        total_votes: Arc<Locked<usize>>,
    ) {
        // Ask for a vote, this could block forever or return nothing at the
        // whims of the network
        let Some((response_term, vote_granted)) = self
            .rpc
            .to(&peer)
            .request_vote(&self.name, election_term)
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
        // Same as request_vote rpc
        let Some(response_term) =
            self.rpc.to(&peer).append_logs(current_term).await
        else {
            return;
        };

        // The heartbeat response contains a higher term, we've fallen behind,
        // become a follower
        if response_term > current_term {
            self.start_follower(response_term).await;
        }
    }

    async fn can_vote_for(&self, node: &str) -> bool {
        let Some(vote) = &*self.last_vote.read().await else {
            return true;
        };
        vote == node
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
        node: &str,
        term: usize,
    ) -> Option<(usize, bool)> {
        self.clone().common_rpc_checks(term).await?;

        // If we can vote for the requesting node, record our vote, reset the
        // election timeout and tell the node
        if self.term.get().await == term && self.can_vote_for(node).await {
            self.last_vote.set(Some(node.to_owned())).await;
            self.last_heartbeat.set(Instant::now()).await;
            return Some((term, true));
        }

        // Otherwise tell it we can't vote
        Some((term, false))
    }

    async fn append_logs(&self, term: usize) -> Option<usize> {
        self.clone().common_rpc_checks(term).await?;

        // If the term matches we can accept the heartbeat
        if self.term.get().await == term {
            match self.role.get().await {
                Role::Follower => (),
                // If we've somehow died while trying to get the term lock, bail
                Role::Dead => return None,
                // If we're recieving heartbeats, clearly somebody else thinks
                // they're the leader, so become a follower
                _ => self.clone().start_follower(term).await,
            }
            // Reset the election timer
            self.last_heartbeat.set(Instant::now()).await;
            // This will probably get more complicated when we do logs...
            return Some(term);
        }

        Some(term)
    }
}
