use macros::spawn;

use crate::network::NetworkHandle;

#[derive(Debug, Clone)]
pub(crate) enum Message {
    RequestVote { node: String, term: usize },
    AppendLogs { term: usize },
}

#[derive(Debug, Clone)]
pub(crate) enum Response {
    Vote { term: usize, granted: bool },
    LogsRecieved { term: usize },
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
    pub(crate) async fn request_vote(
        self,
        node: &str,
        term: usize,
    ) -> Option<(usize, bool)> {
        let msg = Message::RequestVote {
            node: node.to_owned(),
            term,
        };
        #[expect(clippy::match_wildcard_for_single_variants)]
        match self.0.send(self.1, msg).await? {
            Response::Vote { term, granted } => Some((term, granted)),
            resp => {
                println!("Unexpected respone to RequestVote: {resp:?}");
                None
            }
        }
    }

    pub(crate) async fn append_logs(self, term: usize) -> Option<usize> {
        let msg = Message::AppendLogs { term };
        #[expect(clippy::match_wildcard_for_single_variants)]
        match self.0.send(self.1, msg).await? {
            Response::LogsRecieved { term } => Some(term),
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
        node: &str,
        term: usize,
    ) -> Option<(usize, bool)>;
    async fn append_logs(&self, term: usize) -> Option<usize>;
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
            Message::RequestVote { node, term } => {
                let Some((term, granted)) =
                    client.request_vote(&node, term).await
                else {
                    continue;
                };
                let _ = resp.send(Response::Vote { term, granted });
            }
            Message::AppendLogs { term } => {
                let Some(term) = client.append_logs(term).await else {
                    continue;
                };
                let _ = resp.send(Response::LogsRecieved { term });
            }
        }
    }
}
