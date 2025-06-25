use std::{collections::HashMap, sync::Arc};

use tokio::sync::oneshot::Receiver;

use crate::{Node, node::Leadership};

pub(crate) struct Client {
    // Pretend there's networking involved...
    nodes: HashMap<String, Arc<Node>>,
    current_leader: Option<String>,
}

impl Client {
    pub(crate) fn new(nodes: &[Arc<Node>]) -> Self {
        Self {
            nodes: nodes
                .iter()
                .map(|node| (node.name.clone(), node.clone()))
                .collect(),
            current_leader: None,
        }
    }

    pub(crate) async fn get(&mut self, key: String) -> Option<Option<String>> {
        let result = self
            .with_leader(|node| {
                let key = key.clone();
                async move { node.get(key).await }
            })
            .await?;
        result.await.ok()
    }

    pub(crate) async fn set(
        &mut self,
        key: String,
        value: String,
    ) -> Option<()> {
        self.with_leader(|node| {
            let key = key.clone();
            let value = value.clone();
            async move { node.set(key, value).await }
        })
        .await?
        .await
        .ok()
    }

    pub(crate) async fn delete(&mut self, key: String) -> Option<()> {
        self.with_leader(|node| {
            let key = key.clone();
            async move { node.delete(key).await }
        })
        .await?
        .await
        .ok()
    }

    async fn with_leader<T, F: Future<Output = Option<Receiver<T>>>>(
        &mut self,
        f: impl Fn(Arc<Node>) -> F,
    ) -> Option<Receiver<T>> {
        loop {
            if self.current_leader.is_none() {
                self.current_leader = self.find_leader().await;
            }
            let current_leader = self.current_leader.as_ref()?;
            let current_leader = self.nodes.get(current_leader)?.clone();
            if let Some(resp) = f(current_leader).await {
                return Some(resp);
            }
            // Not the leader, retry
            self.current_leader = None;
        }
    }

    async fn find_leader(&self) -> Option<String> {
        let mut candidates = self.nodes.values();
        let mut candidate = candidates.next()?.clone();
        loop {
            match candidate.get_leader().await {
                Leadership::Me => return Some(candidate.name.clone()),
                Leadership::Leader(leader) => {
                    candidate = self.nodes.get(&leader)?.clone();
                    continue;
                }
                Leadership::Unknown => {
                    candidate = candidates.next()?.clone();
                    continue;
                }
            }
        }
    }
}
