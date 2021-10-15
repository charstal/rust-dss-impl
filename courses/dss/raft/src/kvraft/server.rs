use futures::channel::mpsc::{unbounded, UnboundedReceiver};
use futures::executor::ThreadPool;
use futures::task::SpawnExt;
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
// use tokio::runtime::Runtime;
use tokio::sync::{
    oneshot::{self, Sender},
    watch,
};

use crate::proto::kvraftpb::*;
use crate::raft::{self, ApplyMsg};

pub const SNAPSHOT_INTERVAL: u64 = 20;
pub struct KvServer {
    pub rf: raft::Node,
    me: usize,
    // snapshot if log grows this big
    maxraftstate: Option<usize>,
    // Your definitions here.
    // kv store
    kv_store: HashMap<String, String>,
    // record of each clent request seq
    client_seq: HashMap<String, u64>,
    // raft channel
    apply_ch: Option<UnboundedReceiver<ApplyMsg>>,
    // result sender channel
    result_ch: HashMap<u64, Sender<(u64, Option<String>)>>,
}

impl KvServer {
    pub fn new(
        servers: Vec<crate::proto::raftpb::RaftClient>,
        me: usize,
        persister: Box<dyn raft::persister::Persister>,
        maxraftstate: Option<usize>,
    ) -> KvServer {
        // You may need initialization code here.

        let (tx, apply_ch) = unbounded();
        let snapshot = persister.snapshot();
        let rf = raft::Raft::new(servers, me, persister, tx);
        let rf_node = raft::Node::new(rf);

        let mut kvserver = KvServer {
            rf: rf_node,
            me,
            maxraftstate,
            kv_store: HashMap::new(),
            client_seq: HashMap::new(),
            apply_ch: Some(apply_ch),
            result_ch: HashMap::new(),
        };
        kvserver.restore(&snapshot);
        kvserver
    }
}

impl KvServer {
    fn log_compaction(&mut self, applied_index: u64) {
        if let Some(log_size) = self.maxraftstate {
            if !self.rf.check_log_size(log_size) && (applied_index + 1) % SNAPSHOT_INTERVAL == 0 {
                info!("[kvserver] -----------------start snapshot---------------------");
                let snapshot = self.gen_snapshot();
                self.rf.snapshot(applied_index, &snapshot);
            }
        }
    }
    fn gen_snapshot(&self) -> Vec<u8> {
        let kv = self.kv_store.clone();
        let cs = self.client_seq.clone();
        let snapshot = SnapShot {
            key: kv.keys().cloned().collect(),
            value: kv.values().cloned().collect(),
            client_name: cs.keys().cloned().collect(),
            seqs: cs.values().cloned().collect(),
        };
        let mut data = vec![];
        labcodec::encode(&snapshot, &mut data).unwrap();

        data
    }
    fn install_snapshot(&mut self, snapshot: &[u8], term: u64, index: u64) {
        // info!("[kvserver][install snapshot]");
        if snapshot.is_empty() {
            return;
        }
        self.rf.cond_install_snapshot(term, index, snapshot);
        self.restore(snapshot);
    }
    fn restore(&mut self, snapshot: &[u8]) {
        if let Ok(snapshot) = labcodec::decode::<SnapShot>(snapshot) {
            self.kv_store = snapshot
                .key
                .into_iter()
                .zip(snapshot.value.into_iter())
                .collect();
            self.client_seq = snapshot
                .client_name
                .into_iter()
                .zip(snapshot.seqs.into_iter())
                .collect();
        }
    }
}

impl KvServer {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = &self.me;
        let _ = &self.maxraftstate;
    }

    pub fn is_leader(&self) -> bool {
        self.rf.is_leader()
    }

    pub fn get_apply_channel(&mut self) -> UnboundedReceiver<ApplyMsg> {
        self.apply_ch.take().unwrap()
    }

    pub fn get(&self, key: String) -> String {
        match self.kv_store.get(&key) {
            Some(value) => value.clone(),
            None => String::new(),
        }
    }

    pub fn append(&mut self, key: String, value: String) {
        let entry = self.kv_store.entry(key).or_insert_with(|| "".to_string());
        *entry += &value;
    }

    pub fn put(&mut self, key: String, value: String) {
        self.kv_store.insert(key, value);
    }
}

async fn apply_command(server: Arc<Mutex<KvServer>>, stop_signal: Arc<watch::Receiver<bool>>) {
    let mut rx = {
        let mut server = server.lock().unwrap();
        server.get_apply_channel()
    };
    while !*stop_signal.borrow() {
        if let Some(apply_msg) = rx.next().await {
            match apply_msg {
                ApplyMsg::Command { data, index } => {
                    if data.is_empty() {
                        continue;
                    }
                    // info!(
                    //     "[kvserver][rx] apply channel command: {:?} index: {}",
                    //     data, index
                    // );

                    let command = match labcodec::decode::<Command>(&data) {
                        Ok(cmd) => cmd,
                        _ => continue,
                    };
                    let (value, ch) = {
                        let mut server = server.lock().unwrap();

                        let ch = server.result_ch.remove(&index);

                        let entry = server
                            .client_seq
                            .entry(command.identifier.clone())
                            .or_insert(0);
                        // info!(
                        //     "[server]: current seq:{}, command from:{}, {:?}",
                        //     entry, command.identifier, command
                        // );
                        let value = match (command.op, *entry < command.seq) {
                            (3, _) => Some(server.get(command.key)),
                            (2, true) => {
                                server.append(command.key, command.value);
                                // server.client_seq.insert(command.identifier, command.seq);
                                None
                            }
                            (1, true) => {
                                server.put(command.key, command.value);
                                // server.client_seq.insert(command.identifier, command.seq);
                                None
                            }
                            _ => None,
                        };
                        server.client_seq.insert(command.identifier, command.seq);
                        // info!("[server][store]: {:?}", server.kv_store);
                        // info!(
                        //     "[apply_command]: {:?}, {:?}, {:?}",
                        //     server.rf.raft.lock().unwrap(),
                        //     cmd,
                        //     server.kv_store.get(&cmd.key)
                        // );
                        server.log_compaction(index);
                        (value, ch)
                    };

                    if let Some(ch) = ch {
                        ch.send((server.lock().unwrap().rf.term(), value)).unwrap();
                    }
                }
                ApplyMsg::Snapshot { data, term, index } => {
                    let mut server = server.lock().unwrap();
                    server.install_snapshot(&data, term, index);
                }
            }
        }
    }
}

// Choose concurrency paradigm.
//
// You can either drive the kv server by the rpc framework,
//
// ```rust
// struct Node { server: Arc<Mutex<KvServer>> }
// ```
//
// or spawn a new thread runs the kv server and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your definitions here.
    server: Arc<Mutex<KvServer>>,
    thread_pool: Arc<ThreadPool>,

    stop_channel: Arc<watch::Sender<bool>>,
}

impl Node {
    pub fn new(kv: KvServer) -> Node {
        // Your code here.
        // let runtime = Arc::new(Runtime::new().unwrap());
        let server = Arc::new(Mutex::new(kv));
        let thread_pool = Arc::new(ThreadPool::new().unwrap());

        let (tx, rx) = watch::channel(false);
        thread_pool
            .spawn(apply_command(Clone::clone(&server), Arc::new(rx)))
            .unwrap();

        Node {
            server,
            thread_pool,
            stop_channel: Arc::new(tx),
        }
    }

    /// the tester calls kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // If you want to free some resources by `raft::Node::kill` method,
        // you should call `raft::Node::kill` here also to prevent resource leaking.
        // Since the test framework will call kvraft::Node::kill only.
        // self.server.kill();
        // info!("[kvserver] -----------------shutdown server--------------");
        let rf = &self.server.lock().unwrap().rf;
        rf.kill();
        self.stop_channel.send(true).unwrap();
        // Your code here, if desired.
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader()
    }

    pub fn get_state(&self) -> raft::State {
        // Your code here.
        let server = self.server.lock().unwrap();
        raft::State {
            term: server.rf.term(),
            is_leader: server.rf.is_leader(),
        }
    }
}

#[async_trait::async_trait]
impl KvService for Node {
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn get(&self, arg: GetRequest) -> labrpc::Result<GetReply> {
        // Your code here.
        let server = self.server.clone();
        self.thread_pool
            .spawn_with_handle(async move {
                let GetRequest {
                    key,
                    identifier,
                    seq,
                } = arg;
                let mut reply = GetReply {
                    wrong_leader: false,
                    err: "".to_string(),
                    value: "".to_string(),
                };

                let (tx, rx) = oneshot::channel();

                let current_term = {
                    let mut server = server.lock().unwrap();
                    if !server.is_leader() {
                        reply.wrong_leader = true;
                        return Ok(reply);
                    }
                    // let server_seq = server.client_seq.entry(identifier.clone()).or_insert(0);
                    let current_term;
                    // if seq > *server_seq {
                    let cmd = Command {
                        op: 3,
                        key,
                        value: "".to_string(),
                        identifier,
                        seq,
                    };

                    match server.rf.start(&cmd) {
                        Err(_) => {
                            reply.wrong_leader = true;
                            return Ok(reply);
                        }
                        Ok((index, term)) => {
                            server.result_ch.insert(index, tx);
                            current_term = term;
                        }
                    }
                    // }
                    current_term
                };
                match rx.await {
                    Ok((term, value)) => {
                        if current_term != term {
                            reply.err = "leader changed".to_string();
                        } else {
                            reply.value = value.unwrap_or_else(|| "".to_string());
                        }
                    }
                    Err(_) => {
                        return Err(labrpc::Error::Recv(futures::channel::oneshot::Canceled));
                    }
                }
                Ok(reply)
            })
            .unwrap()
            .await
        // hanlder.unwrap().await
    }

    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn put_append(&self, arg: PutAppendRequest) -> labrpc::Result<PutAppendReply> {
        // Your code here.
        let server = self.server.clone();
        self.thread_pool
            .spawn_with_handle(async move {
                let PutAppendRequest {
                    key,
                    value,
                    op,
                    identifier,
                    seq,
                } = arg;
                let mut reply = PutAppendReply {
                    wrong_leader: false,
                    err: "".to_string(),
                };
                let (tx, rx) = oneshot::channel();

                let current_term = {
                    let mut server = server.lock().unwrap();
                    if !server.is_leader() {
                        reply.wrong_leader = true;
                        return Ok(reply);
                    }
                    // let server_seq = server.client_seq.entry(identifier.clone()).or_insert(0);
                    let current_term;
                    // if seq > *server_seq {
                    let cmd = Command {
                        op,
                        key,
                        value,
                        identifier,
                        seq,
                    };
                    match server.rf.start(&cmd) {
                        Err(_) => {
                            reply.wrong_leader = true;
                            return Ok(reply);
                        }
                        Ok((index, term)) => {
                            server.result_ch.insert(index, tx);
                            current_term = term;
                        }
                    }
                    // }
                    current_term
                };
                match rx.await {
                    Ok((term, _)) => {
                        if current_term != term {
                            reply.err = "leader changed".to_string();
                        }
                    }
                    Err(_e) => {
                        return Err(labrpc::Error::Recv(futures::channel::oneshot::Canceled));
                    }
                }

                Ok(reply)
            })
            .unwrap()
            .await
    }
}
