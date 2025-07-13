use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::Result;
use futures::future;

use crate::{DataStore, InProcessNetwork, Node, node::Leadership};

trait Join {
    type Result;
    async fn join(self) -> Vec<Self::Result>;
}

impl<I, F> Join for I
where
    I: IntoIterator<Item = F>,
    F: Future,
{
    type Result = F::Output;

    async fn join(self) -> Vec<Self::Result> {
        future::join_all(self).await
    }
}

static MAX_TERM_DURATION: Duration = Duration::from_millis(300);

async fn create_cluster(
    server_count: usize,
    network: &InProcessNetwork,
) -> Result<Vec<Arc<Node>>> {
    let nodes = (0..server_count).map(|i| i.to_string()).collect::<Vec<_>>();
    let nodes = nodes
        .iter()
        .map(|node| {
            Ok(Node::new(
                node,
                nodes.iter().cloned(),
                network.handle(node)?,
                DataStore::new(),
                false,
            ))
        })
        .collect::<Result<Vec<_>>>()?
        .join()
        .await;
    for node in &nodes {
        node.clone().start().await;
    }
    tokio::time::sleep(MAX_TERM_DURATION).await;
    Ok(nodes)
}

async fn assert_one_leader_in_all_active_terms(nodes: &[Arc<Node>]) -> String {
    let leaders_and_terms = nodes
        .iter()
        .map(|node| async move {
            match node.get_leader().await {
                Leadership::Me => {
                    Some((node.name.clone(), node.get_term().await))
                }
                Leadership::Leader(leader) => {
                    Some((leader, node.get_term().await))
                }
                Leadership::Unknown => None,
            }
        })
        .join()
        .await
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    let mut leadership = HashMap::<_, HashSet<_>>::new();
    for (leader, term) in leaders_and_terms {
        let _ = leadership.entry(term).or_default().insert(leader);
    }

    let mut max_term: isize = -1;
    let mut max_term_leader = String::new();
    for (term, leaders) in leadership {
        assert!(
            leaders.len() == 1,
            "Term {term} reports {} leaders: {leaders:?}",
            leaders.len()
        );
        #[expect(clippy::cast_possible_wrap)]
        if term as isize > max_term {
            max_term = term as isize;
            max_term_leader = leaders.into_iter().next().unwrap();
        }
    }
    max_term_leader
}

async fn assert_terms_agree(nodes: &[Arc<Node>]) -> usize {
    let terms = nodes
        .iter()
        .map(|node| node.get_term())
        .join()
        .await
        .into_iter()
        .collect::<HashSet<_>>();
    assert_eq!(
        terms.len(),
        1,
        "Nodes reporting conflicting terms {terms:?}",
    );
    terms.into_iter().next().unwrap()
}

mod three_a {
    use rand::seq::IndexedRandom;

    use super::*;

    #[tokio::test]
    async fn initial_election() -> Result<()> {
        let nodes = create_cluster(3, &InProcessNetwork::new().await).await?;

        let _ = assert_one_leader_in_all_active_terms(&nodes).await;

        let term1 = assert_terms_agree(&nodes).await;
        assert!(term1 >= 1);

        tokio::time::sleep(MAX_TERM_DURATION * 2).await;

        let term2 = assert_terms_agree(&nodes).await;
        assert_eq!(term1, term2);

        let _ = assert_one_leader_in_all_active_terms(&nodes).await;

        Ok(())
    }

    #[tokio::test]
    async fn reelection() -> Result<()> {
        let network = InProcessNetwork::new().await;
        let nodes = create_cluster(3, &network).await?;

        let leader1 = assert_one_leader_in_all_active_terms(&nodes).await;

        network.isolate(&leader1)?;
        let _ = assert_one_leader_in_all_active_terms(&nodes).await;

        network.repair(&leader1)?;
        let _ = assert_one_leader_in_all_active_terms(&nodes).await;

        let (connected_node, disconnected_nodes) = {
            let mut nodes = nodes.iter();
            let connected_node = nodes.next().unwrap();
            let disconnected_nodes =
                [nodes.next().unwrap(), nodes.next().unwrap()];
            (connected_node, disconnected_nodes)
        };

        for node in disconnected_nodes {
            network.isolate(&node.name)?;
        }

        assert!(!matches!(connected_node.get_leader().await, Leadership::Me));

        network.repair(&disconnected_nodes[0].name)?;
        let _ = assert_one_leader_in_all_active_terms(&nodes).await;

        network.repair(&disconnected_nodes[1].name)?;
        let _ = assert_one_leader_in_all_active_terms(&nodes).await;

        Ok(())
    }

    #[tokio::test]
    async fn many_elections() -> Result<()> {
        let network = InProcessNetwork::new().await;
        let nodes = create_cluster(7, &network).await?;

        let _ = assert_one_leader_in_all_active_terms(&nodes).await;

        for _ in 0..10 {
            for node in nodes.choose_multiple(&mut rand::rng(), 3) {
                network.isolate(&node.name)?;
            }

            let _ = assert_one_leader_in_all_active_terms(&nodes).await;

            for node in &nodes {
                network.repair(&node.name)?;
            }
        }

        let _ = assert_one_leader_in_all_active_terms(&nodes).await;

        Ok(())
    }
}
