use std::time::Duration;
use std::{
    fmt,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc::channel,
    },
};
use uuid::Uuid;

use crate::proto::kvraftpb::*;

enum Op {
    Put(String, String),
    Append(String, String),
}

pub struct Clerk {
    pub name: String,
    pub servers: Vec<KvClient>,
    // You will have to modify this struct.
    last_leader: AtomicUsize,
    // unique identifiers
    identifier: String,
    // start from 1
    seq: AtomicU64,
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk").field("name", &self.name).finish()
    }
}

impl Clerk {
    pub fn new(name: String, servers: Vec<KvClient>) -> Clerk {
        // You'll have to add code here.
        Clerk {
            name,
            servers,
            last_leader: AtomicUsize::new(0),
            identifier: Uuid::new_v4().to_string(),
            seq: AtomicU64::new(1),
        }
    }

    fn update_leader(&self) {
        let leader_id = self.last_leader.load(Ordering::Relaxed);
        self.last_leader
            .store((leader_id + 1) % self.servers.len(), Ordering::Relaxed);
    }

    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    //
    // you can send an RPC with code like this:
    // if let Some(reply) = self.servers[i].get(args).wait() { /* do something */ }
    pub fn get(&self, key: String) -> String {
        // You will have to modify this function.
        let request = GetRequest {
            key,
            identifier: self.identifier.clone(),
            seq: self.seq.fetch_add(1, Ordering::Relaxed),
        };
        loop {
            let leader_id = self.last_leader.load(Ordering::Relaxed);
            let server = &self.servers[leader_id];
            let server_clone = server.clone();
            let (tx, rx) = channel();
            let args = request.clone();
            server.spawn(async move {
                let reply = server_clone.get(&args).await;
                tx.send(reply).unwrap_or(());
            });
            let reply = rx.recv_timeout(Duration::from_millis(1000));
            // info!(
            //     "[client][get]: client:{:?}, request:{:?}, leader:{}, response:{:?}",
            //     self.name, request, leader_id, reply
            // );
            match reply {
                Ok(Ok(reply)) => {
                    // info!(
                    //     "[client][get][info]: client name: {} send:{:?}, response: {:?}",
                    //     self.name, request, reply
                    // );
                    if !reply.wrong_leader {
                        if reply.err.is_empty() {
                            return reply.value;
                        } else {
                            error!("[client][get][error]: error:{}", reply.err);
                        }
                    } else {
                        self.update_leader();
                    }
                }
                // Ok(Err(_)) => {
                //     self.update_leader();
                // }
                _ => {
                    self.update_leader();
                }
            }
        }
    }

    /// shared by Put and Append.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].put_append(args).unwrap();
    fn put_append(&self, op: Op) {
        // You will have to modify this function.
        let (op_idx, key, value) = match op {
            Op::Put(key, value) => (1, key, value),
            Op::Append(key, value) => (2, key, value),
        };
        let request = PutAppendRequest {
            key,
            value,
            op: op_idx,
            identifier: self.identifier.clone(),
            seq: self.seq.fetch_add(1, Ordering::Relaxed),
        };

        loop {
            let leader_id = self.last_leader.load(Ordering::Relaxed);
            let server = &self.servers[leader_id];
            let server_clone = server.clone();
            let (tx, rx) = channel();
            let args = request.clone();
            server.spawn(async move {
                let reply = server_clone.put_append(&args).await;
                tx.send(reply).unwrap_or(());
            });
            let reply = rx.recv_timeout(Duration::from_millis(500));
            // info!(
            //     "[client][put_append]: request:{:?}, leader:{}, response:{:?}",
            //     request, leader_id, reply
            // );
            match reply {
                Ok(Ok(reply)) => {
                    // info!(
                    //     "[client][put or append][info]: client name: {}, send:{:?}, response: {:?}",
                    //     self.name, request, reply
                    // );
                    if !reply.wrong_leader {
                        if reply.err.is_empty() {
                            return;
                        } else {
                            error!("[client][put or append][error]:{}", reply.err);
                        }
                    } else {
                        self.update_leader();
                    }
                }
                // Ok(Err(_)) => {

                // }
                _ => {
                    self.update_leader();
                }
            }
        }
    }

    pub fn put(&self, key: String, value: String) {
        self.put_append(Op::Put(key, value))
    }

    pub fn append(&self, key: String, value: String) {
        self.put_append(Op::Append(key, value))
    }
}
