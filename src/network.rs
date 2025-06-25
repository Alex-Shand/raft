use std::{
    collections::{HashMap, HashSet},
    mem,
    sync::Arc,
};

use anyhow::{Result, anyhow, ensure};
use macros::spawn;
use tokio::{
    select,
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        oneshot::Sender,
    },
};

use crate::{
    rpc::{Message, Response},
    util::Locked,
};

struct Request {
    to: String,
    msg: Message,
    resp: Sender<Response>,
}

struct Mailbox {
    name: String,
    outbox: Locked<UnboundedReceiver<Request>>,
    inbox: UnboundedSender<Request>,
}

enum Command {
    NewMailbox(Mailbox),
    Isolate(String),
    Repair(String),
    Partition(Vec<String>),
    DropChance(u32),
    Departition,
}

pub(crate) struct InProcessNetwork {
    to_worker: UnboundedSender<Command>,
    drop_chance: Arc<Locked<u32>>,
}

impl InProcessNetwork {
    pub(crate) async fn new() -> Self {
        let (send, recv) = tokio::sync::mpsc::unbounded_channel();
        start_worker(recv).await;
        Self {
            to_worker: send,
            drop_chance: Arc::new(Locked::new(0)),
        }
    }

    pub(crate) fn isolate(&self, node: &str) -> Result<()> {
        self.to_worker
            .send(Command::Isolate(node.to_owned()))
            .map_err(|_| anyhow!("Failed to isolate {node}"))
    }

    pub(crate) fn repair(&self, node: &str) -> Result<()> {
        self.to_worker
            .send(Command::Repair(node.to_owned()))
            .map_err(|_| anyhow!("Failed to repair {node}"))
    }

    pub(crate) fn partition(&self, nodes: &[String]) -> Result<()> {
        self.to_worker
            .send(Command::Partition(nodes.to_owned()))
            .map_err(|_| anyhow!("Failed to partition nodes: {nodes:?}"))
    }

    pub(crate) fn departition(&self) -> Result<()> {
        self.to_worker
            .send(Command::Departition)
            .map_err(|_| anyhow!("Failed to remove partition"))
    }

    pub(crate) async fn set_drop_chance(
        &mut self,
        percentage: u32,
    ) -> Result<()> {
        ensure!(percentage < 100);
        self.drop_chance.set(percentage).await;
        self.to_worker
            .send(Command::DropChance(percentage))
            .map_err(|_| anyhow!("Failed to degrade network"))
    }

    pub(crate) fn handle(&self, me: &str) -> Result<NetworkHandle> {
        let (out_send, out_recv) = tokio::sync::mpsc::unbounded_channel();
        let (in_send, in_recv) = tokio::sync::mpsc::unbounded_channel();
        self.to_worker
            .send(Command::NewMailbox(Mailbox {
                name: me.to_owned(),
                outbox: Locked::new(out_recv),
                inbox: in_send,
            }))
            .map_err(|_| anyhow!("Failed to send mailbox to worker"))?;
        Ok(NetworkHandle {
            drop_chance: self.drop_chance.clone(),
            outbox: out_send,
            inbox: Arc::new(Locked::new(in_recv)),
        })
    }
}

#[derive(Clone)]
pub(crate) struct NetworkHandle {
    drop_chance: Arc<Locked<u32>>,
    outbox: UnboundedSender<Request>,
    inbox: Arc<Locked<UnboundedReceiver<Request>>>,
}

impl NetworkHandle {
    pub(crate) async fn send(
        &self,
        to: &str,
        message: Message,
    ) -> Option<Response> {
        // For the response
        let (send, recv) = tokio::sync::oneshot::channel();
        // Dispatch the message
        self.outbox
            .send(Request {
                to: to.to_owned(),
                msg: message,
                resp: send,
            })
            .ok()?;

        // The response might not make it
        if should_drop(self.drop_chance.get().await) {
            return None;
        }

        // And wait for a response
        recv.await.ok()
    }

    pub(crate) async fn recv(&self) -> Option<(Message, Sender<Response>)> {
        let request = self.inbox.write().await.recv().await?;
        Some((request.msg, request.resp))
    }
}

#[spawn]
async fn start_worker(commands: UnboundedReceiver<Command>) -> ! {
    // For some reason having the parameter be mut commands triggers the unused
    // mut lint
    let mut commands = commands;
    let mut mailboxes = HashMap::new();
    let mut isolated = HashSet::new();
    let mut partition = HashSet::new();
    let mut drop_chance = 0;
    loop {
        select! {
            Some(command) = commands.recv() => {
                match command {
                    Command::NewMailbox(mailbox) => {
                        let _ = mailboxes.insert(mailbox.name.clone(), mailbox);
                    },
                    Command::Isolate(node) => {
                        let _ = isolated.insert(node);
                    }
                    Command::Repair(node) => {
                        let _ = isolated.remove(&node);
                    }
                    Command::Partition(nodes) => {
                        partition = nodes.into_iter().collect();
                    }
                    Command::Departition => {
                        partition.clear();
                    }
                    Command::DropChance(p) => {
                        drop_chance = p;
                    }
                }
            }
            () = dispatch_mail(&mailboxes, &isolated, &partition, drop_chance) => {
            }
        }
    }
}

async fn dispatch_mail(
    mailboxes: &HashMap<String, Mailbox>,
    isolated: &HashSet<String>,
    partition: &HashSet<String>,
    drop_chance: u32,
) {
    for mailbox in mailboxes.values() {
        if let Ok(request) = mailbox.outbox.write().await.try_recv() {
            // If this message is coming from an isolated node, drop it
            if isolated.contains(&mailbox.name) {
                continue;
            }

            // If this message is going to an isolated node, drop it
            if isolated.contains(&request.to) {
                continue;
            }

            // If this message crosses the partition, drop it
            if crosses(partition, &mailbox.name, &request.to) {
                continue;
            }

            // Finally we have a random chance to drop the message
            if should_drop(drop_chance) {
                // If we drop the response channel then the reciever end will
                // immediatly return None when polled, if we leak it instead
                // it'll block forever. Try both
                if rand::random_ratio(1, 2) {
                    mem::forget(request.resp);
                }
                continue;
            }

            let inbox = &mailboxes[&request.to].inbox;
            let _ = inbox.send(request);
        }
    }
}

fn crosses(partition: &HashSet<String>, src: &str, dst: &str) -> bool {
    partition.contains(src) != partition.contains(dst)
}

fn should_drop(drop_chance: u32) -> bool {
    if drop_chance == 0 {
        return false;
    }
    rand::random_ratio(drop_chance, 100)
}
