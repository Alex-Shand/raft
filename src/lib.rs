//! raft
#![warn(elided_lifetimes_in_paths)]
#![warn(missing_docs)]
#![warn(unreachable_pub)]
#![warn(unused_crate_dependencies)]
#![warn(unused_import_braces)]
#![warn(unused_lifetimes)]
#![warn(unused_qualifications)]
#![deny(unsafe_code)]
#![deny(unsafe_op_in_unsafe_fn)]
#![deny(unused_results)]
#![deny(missing_debug_implementations)]
#![deny(missing_copy_implementations)]
#![warn(clippy::pedantic)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::let_underscore_untyped)]
#![allow(clippy::similar_names)]
#![allow(clippy::result_large_err)]
#![allow(clippy::struct_field_names)]
#![allow(clippy::missing_errors_doc)]

use std::{
    io::{self, Write},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use anyhow::{Result, bail};
use futures::future;
use tokio::select;
use util::Race;

use self::{network::InProcessNetwork, node::Node};

mod network;
mod node;
mod rpc;
mod util;

/// raft
#[allow(missing_copy_implementations)]
#[derive(Debug, argh::FromArgs)]
pub struct Args {
    /// don't start repl
    #[argh(switch)]
    norepl: bool,
    /// every node chooses the same election timeout
    #[argh(switch)]
    thrash: bool,
}

#[allow(missing_docs)]
#[allow(clippy::missing_errors_doc)]
#[allow(clippy::missing_panics_doc)]
pub async fn main(Args { norepl, thrash }: Args) -> Result<()> {
    let network = InProcessNetwork::new().await;
    let nodes = ["A", "B", "C", "D", "E"];
    let nodes = nodes
        .iter()
        .map(|node| {
            Ok(Node::new(
                *node,
                nodes.iter().copied(),
                network.handle(node)?,
                thrash,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let nodes = future::join_all(nodes.into_iter()).await;
    for node in &nodes {
        node.clone().start().await;
    }
    if norepl {
        tokio::signal::ctrl_c().await?;
    } else {
        start_repl(&nodes, network).await;
    }
    Ok(())
}

enum Command {
    Status,
    Watch,
    Start(String),
    Kill(String),
    Reset,
    Isolate(String),
    Repair(String),
    Partition(Vec<String>),
    Departition,
    Degrade(u32),
    Fix,
    Thrash(bool),
    Quit,
}

impl FromStr for Command {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let words = s.split_whitespace().collect::<Vec<_>>();
        match &words[..] {
            ["status"] => Ok(Command::Status),
            ["watch"] => Ok(Command::Watch),
            ["start", node] => Ok(Command::Start((*node).to_owned())),
            ["kill", node] => Ok(Command::Kill((*node).to_owned())),
            ["reset"] => Ok(Command::Reset),
            ["isolate", node] => Ok(Command::Isolate((*node).to_owned())),
            ["repair", node] => Ok(Command::Repair((*node).to_owned())),
            ["partition", nodes @ ..] => Ok(Command::Partition(
                nodes.iter().map(ToString::to_string).collect(),
            )),
            ["departition"] => Ok(Command::Departition),
            ["degrade", percentage] => {
                Ok(Command::Degrade(percentage.parse()?))
            }
            ["fix"] => Ok(Command::Fix),
            ["thrash", "on"] => Ok(Command::Thrash(true)),
            ["thrash", "off"] => Ok(Command::Thrash(false)),
            ["quit" | "exit"] => Ok(Command::Quit),
            words => bail!("Invalid command: {words:?}"),
        }
    }
}

async fn start_repl(nodes: &[Arc<Node>], mut network: InProcessNetwork) {
    let mut buf = String::new();
    loop {
        buf.clear();
        print!("> ");
        io::stdout().flush().expect("AHHHHHHHHHHHHHHHHHHHHHHHHHH!");
        let _ = io::stdin().read_line(&mut buf).expect("AHHHHHHH!");
        let command = match buf.parse() {
            Ok(command) => command,
            Err(e) => {
                println!("{e}");
                continue;
            }
        };
        match command {
            Command::Status => print_status(nodes).await,
            Command::Watch => {
                print_status(nodes).await;
                loop {
                    select! {
                        _ = tokio::signal::ctrl_c() => {
                            break;
                        }
                        () = tokio::time::sleep(Duration::from_secs(1)) => {
                            println!("----------------------------------------");
                            print_status(nodes).await;
                        }
                    }
                }
            }
            Command::Start(to_start) => {
                for node in nodes {
                    if node.name == to_start {
                        node.clone().start().await;
                    }
                }
            }
            Command::Kill(to_kill) => {
                for node in nodes {
                    if node.name == to_kill {
                        node.kill().await;
                    }
                }
            }
            Command::Reset => {
                nodes.iter().map(|node| node.kill()).race().await;
                nodes.iter().map(|node| node.clone().start()).race().await;
            }
            Command::Isolate(node) => match network.isolate(&node) {
                Ok(()) => (),
                Err(e) => println!("{e}"),
            },
            Command::Repair(node) => match network.repair(&node) {
                Ok(()) => (),
                Err(e) => println!("{e}"),
            },
            Command::Partition(nodes) => match network.partition(&nodes) {
                Ok(()) => (),
                Err(e) => println!("{e}"),
            },
            Command::Departition => match network.departition() {
                Ok(()) => (),
                Err(e) => println!("{e}"),
            },
            Command::Degrade(percentage) => {
                match network.set_drop_chance(percentage).await {
                    Ok(()) => (),
                    Err(e) => println!("{e}"),
                }
            }
            Command::Fix => match network.set_drop_chance(0).await {
                Ok(()) => (),
                Err(e) => println!("{e}"),
            },
            Command::Thrash(thrash) => {
                nodes
                    .iter()
                    .map(|node| node.set_thrash(thrash))
                    .race()
                    .await;
            }
            Command::Quit => return,
        }
    }
}

async fn print_status(nodes: &[Arc<Node>]) {
    for node in nodes {
        println!("{} {:?}", node.name, node.status().await);
    }
}
