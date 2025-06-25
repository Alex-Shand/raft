use std::time::Duration;

use macros::spawn;
use tokio::select;

use crate::{network::NetworkHandle, node::LogEntry};

#[derive(Debug, Clone)]
pub(crate) enum Message {
    RequestVote {
        node: String,
        term: usize,
        last_log_index: usize,
        last_log_term: usize,
    },
    AppendLogs {
        term: usize,
        leader: String,
        prev_index: usize,
        prev_term: usize,
        entries: Vec<LogEntry>,
        leader_commit: usize,
    },
}

#[derive(Debug, Clone)]
pub(crate) enum Response {
    Vote { term: usize, granted: bool },
    LogsRecieved { term: usize, success: bool },
}

pub(crate) struct RpcClient(NetworkHandle);

impl RpcClient {
    pub(crate) fn new(network: NetworkHandle) -> Self {
        Self(network)
    }

    pub(crate) fn to<'a>(&'a self, to: &'a str) -> Peer<'a> {
        Peer(&self.0, to)
    }
}

pub(crate) struct Peer<'a>(&'a NetworkHandle, &'a str);

impl Peer<'_> {
    async fn send(self, msg: Message) -> Option<Response> {
        select! {
            () = tokio::time::sleep(Duration::from_secs(1)) => {
                None
            }
            resp = self.0.send(self.1, msg) => {
                resp
            }
        }
    }

    pub(crate) async fn request_vote(
        self,
        node: &str,
        term: usize,
        last_log_index: usize,
        last_log_term: usize,
    ) -> Option<(usize, bool)> {
        let msg = Message::RequestVote {
            node: node.to_owned(),
            term,
            last_log_index,
            last_log_term,
        };
        #[expect(clippy::match_wildcard_for_single_variants)]
        match self.send(msg).await? {
            Response::Vote { term, granted } => Some((term, granted)),
            resp => {
                println!("Unexpected respone to RequestVote: {resp:?}");
                None
            }
        }
    }

    pub(crate) async fn append_logs(
        self,
        term: usize,
        leader: &str,
        prev_index: usize,
        prev_term: usize,
        entries: &[LogEntry],
        leader_commit: usize,
    ) -> Option<(usize, bool)> {
        let msg = Message::AppendLogs {
            term,
            leader: leader.to_owned(),
            prev_index,
            prev_term,
            entries: entries.to_owned(),
            leader_commit,
        };
        #[expect(clippy::match_wildcard_for_single_variants)]
        match self.send(msg).await? {
            Response::LogsRecieved { term, success } => Some((term, success)),
            resp => {
                println!("Unexpected respone to AppendLogs: {resp:?}");
                None
            }
        }
    }
}

#[async_trait::async_trait]
pub(crate) trait RpcServer {
    async fn request_vote(
        &self,
        node: String,
        term: usize,
        last_log_index: usize,
        last_log_term: usize,
    ) -> Option<(usize, bool)>;
    async fn append_logs(
        &self,
        term: usize,
        leader: String,
        prev_index: usize,
        prev_term: usize,
        entries: Vec<LogEntry>,
        leader_commit: usize,
    ) -> Option<(usize, bool)>;
}

#[spawn]
pub(crate) async fn start_rpc(
    network: NetworkHandle,
    client: impl RpcServer + Send + Sync + 'static,
) -> ! {
    loop {
        let Some((msg, resp)) = network.recv().await else {
            continue;
        };
        match msg {
            Message::RequestVote {
                node,
                term,
                last_log_index,
                last_log_term,
            } => {
                let Some((term, granted)) = client
                    .request_vote(node, term, last_log_index, last_log_term)
                    .await
                else {
                    continue;
                };
                let _ = resp.send(Response::Vote { term, granted });
            }
            Message::AppendLogs {
                term,
                leader,
                prev_index,
                prev_term,
                entries,
                leader_commit,
            } => {
                let Some((term, success)) = client
                    .append_logs(
                        term,
                        leader,
                        prev_index,
                        prev_term,
                        entries,
                        leader_commit,
                    )
                    .await
                else {
                    continue;
                };
                let _ = resp.send(Response::LogsRecieved { term, success });
            }
        }
    }
}
