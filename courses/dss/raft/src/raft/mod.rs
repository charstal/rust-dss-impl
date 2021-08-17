use futures::channel::mpsc::UnboundedSender;
use rand::{self, Rng};
use std::cmp::min;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::runtime;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::time;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
// use crate::kvraft::client;
use crate::proto::raftpb::*;

pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

#[derive(Debug, PartialEq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    term: u64,
    index: u64,
    command: Vec<u8>,
}

impl LogEntry {
    pub fn new(entry: append_entries_args::Entry) -> Self {
        LogEntry {
            term: entry.term,
            index: entry.index,
            command: entry.command,
        }
    }

    pub fn from_parameter(term: u64, index: u64, command: Vec<u8>) -> Self {
        LogEntry {
            term,
            index,
            command,
        }
    }

    pub fn get_term(&self) -> u64 {
        self.term
    }
    pub fn get_index(&self) -> u64 {
        self.index
    }
    pub fn get_command(&self) -> Vec<u8> {
        self.command.clone()
    }
}

#[derive(Debug, Clone, Default)]
pub struct ElectionTimeOut {
    duration: time::Duration,
}

impl ElectionTimeOut {
    fn generate_new_duration() -> time::Duration {
        let duration = rand::thread_rng().gen_range(300, 500);
        time::Duration::from_millis(duration)
    }

    pub fn new() -> Self {
        ElectionTimeOut {
            duration: Self::generate_new_duration(),
        }
    }
    pub fn next(&mut self) {
        self.duration = Self::generate_new_duration();
    }

    pub fn get_timeout(&self) -> &time::Duration {
        &self.duration
    }
}

// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    // state: Arc<State>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    voted_for: Option<usize>,
    // current_term init by 0
    current_term: u64,

    // role
    role: Role,
    // index of highest log entry
    commit_index: u64,
    // index of hightest log entry applied
    last_applied: u64,
    // index of the next log entry to send to
    next_index: Vec<u64>,
    // index of highest log entry known to be replicated on server
    match_index: Vec<u64>,
    // log
    logs: Vec<LogEntry>,
    // send newly committed messages
    apply_ch: UnboundedSender<ApplyMsg>,
    // time log
    last_receive_time: time::Instant,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();
        let num_of_peers = peers.len();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            // state: Arc::default(),
            voted_for: None,
            current_term: 0,
            role: Role::Follower,
            commit_index: 0,
            last_applied: 0,

            next_index: vec![0; num_of_peers],
            match_index: vec![0; num_of_peers],

            // fill one replacement, start from one
            logs: vec![LogEntry {
                term: 0,
                index: 0,
                command: vec![],
            }],
            apply_ch,
            last_receive_time: time::Instant::now(),
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            // return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    fn send_request_vote(
        &self,
        server: usize,
        args: RequestVoteArgs,
    ) -> oneshot::Receiver<Result<RequestVoteReply>> {
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let peer = &self.peers[server];
        // let peer_clone = peer.clone();
        // let (tx, rx) = channel();
        // peer.spawn(async move {
        //     let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
        //     tx.send(res);
        // });
        // rx
        // ```
        let peer = &self.peers[server];
        let peer_clone = peer.clone();

        let (tx, rx) = oneshot::channel();
        peer.spawn(async move {
            let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
            tx.send(res).unwrap_or(()); // 即使请求失败，也不能panic
        });
        rx
    }

    fn request_vote(&mut self, args: &RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        // 1. Reply false if term < currentTerm
        // 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date
        //    as receiver’s log, grant vote

        let current_term = self.current_term;

        // If RPC request or response contains term T > currentTerm:
        // set currentTerm = T, convert to follower
        if current_term < args.term {
            self.convert_to_follower(args.term)
        }

        let vote_granted = if args.term < current_term {
            false
        } else {
            self.voted_for.map_or(true, |candidate_id| {
                if candidate_id != args.candidate_id as usize {
                    return false;
                }
                let last = self.logs.last().map_or((0, 0), |log| (log.term, log.index));
                (args.last_log_term, args.last_log_index) >= last
            })
        };

        if vote_granted {
            self.voted_for = Some(args.candidate_id as usize);
            self.update_receiver_time();
        }

        Ok(RequestVoteReply {
            term: current_term,
            vote_granted,
        })
    }

    fn send_append_entries(
        &self,
        server: usize,
        args: AppendEntriesArgs,
    ) -> oneshot::Receiver<Result<AppendEntriesReply>> {
        let peer = &self.peers[server];
        let peer_clone = peer.clone();

        let (tx, rx) = oneshot::channel();
        peer.spawn(async move {
            let res = peer_clone.append_entries(&args).await.map_err(Error::Rpc);
            tx.send(res).unwrap_or(()); // 即使请求失败，也不能panic
        });
        rx
    }

    fn append_entries(&mut self, args: &AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        self.update_receiver_time();

        let mut success = true;
        let current_term = self.current_term;
        if current_term < args.term {
            self.convert_to_follower(args.term);
        }
        if current_term > args.term {
            success = false;
        }

        if self.role == Role::Candidate {
            self.convert_to_follower(args.term);
        }
        if args.prev_log_index > 0 {
            if let Some(log) = self.get_log_entry(args.prev_log_index as usize) {
                if log.get_term() != args.prev_log_term {
                    success = false;
                    self.truncate_log(log.get_index() as usize);
                }
            } else {
                success = false;
            }
        }

        for entry in args.entries.iter() {
            if let Some(log) = self.get_log_entry(entry.index as usize) {
                if log.term != entry.term {
                    self.truncate_log(log.get_index() as usize);
                    self.logs.push(LogEntry::new(entry.to_owned()));
                }
            } else {
                self.logs.push(LogEntry::new(entry.to_owned()));
            }
        }

        if args.leader_commit > self.commit_index {
            self.commit_index = min(args.leader_commit, self.last_log_index());
        }

        Ok(AppendEntriesReply {
            term: current_term,
            success,
        })
    }

    fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        if self.role == Role::Leader {
            let index = self.last_log_index() + 1;
            let term = self.current_term;
            let mut buf = vec![];
            labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
            // Your code here (2B).
            let log_entry = LogEntry::from_parameter(term, index as u64, buf);
            info!("recevice from client {:?}", log_entry);
            self.logs.push(log_entry);
            self.next_index[self.me] = index as u64;

            Ok((index as u64, term))
        } else {
            Err(Error::NotLeader)
        }
    }

    fn update_receiver_time(&mut self) {
        self.last_receive_time = time::Instant::now();
    }

    fn convert_to_follower(&mut self, term: u64) {
        // set currentTerm = term, convert to follower
        self.role = Role::Follower;
        self.voted_for = None;
        self.current_term = term;
    }

    fn convert_to_candidate(&mut self) {
        // set currentTerm = term, convert to follower
        self.role = Role::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.me);
    }

    fn covert_to_leader(&mut self) {
        self.role = Role::Leader;
        for i in 0..self.peers.len() {
            self.next_index[i] = self.last_log_index() + 1;
        }
    }

    fn get_log_entry(&self, index: usize) -> Option<LogEntry> {
        match index {
            0 => None,
            idx => self.logs.get(idx).map(|x| x.to_owned()),
        }
    }

    fn last_log_index(&self) -> u64 {
        self.logs.last().map_or(1, |log| log.get_index())
    }

    fn last_log_term(&self) -> u64 {
        self.logs.last().map_or(0, |log| log.get_term())
    }

    fn generate_append_entries_rpc_request(&self, peer: usize) -> AppendEntriesArgs {
        let mut args = AppendEntriesArgs {
            leader_id: self.me as u64,
            term: self.current_term,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: self.commit_index,
        };

        let pre_next_index = self.next_index[peer];
        if let Some(prev_log) = self.get_log_entry((pre_next_index - 1) as usize) {
            args.prev_log_index = prev_log.index;
            args.prev_log_term = prev_log.term;
        }

        for idx in pre_next_index..=self.last_log_index() {
            let log = self.get_log_entry(idx as usize).unwrap();
            args.entries.push(append_entries_args::Entry {
                term: log.get_term(),
                index: log.get_index(),
                command: log.get_command(),
            });
        }

        args
    }

    fn generate_request_vote_rpc_request(&self) -> RequestVoteArgs {
        RequestVoteArgs {
            term: self.current_term,
            candidate_id: self.me as u64,
            last_log_index: self.last_log_index(),
            last_log_term: self.last_log_term(),
        }
    }

    fn truncate_log(&mut self, index: usize) {
        self.logs.drain(index..);
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.send_request_vote(0, Default::default());
        self.persist();
        // let _ = &self.state;
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
        let _ = &self.apply_ch;
        let _ = &self.current_term;
        let _ = &self.role;
        let _ = &self.voted_for;
        let _ = &self.commit_index;
        let _ = &self.last_applied;

        let _ = &self.next_index;
        let _ = &self.match_index;
        let _ = &self.logs;
    }
}

async fn leader_election(raft: Arc<Mutex<Raft>>, stop_signal: watch::Receiver<bool>) {
    // info!("Start leader election");
    let mut timeout = ElectionTimeOut::new();
    while !*stop_signal.borrow() {
        // let election_timeout = rand::thread_rng().gen_range(300, 500);
        // let election_timeout = time::Duration::from_millis(election_timeout);

        let rf = Clone::clone(&raft);

        {
            let mut raft = raft.lock().unwrap();
            let now = time::Instant::now();
            if raft.role != Role::Leader && now - raft.last_receive_time > *timeout.get_timeout() {
                timeout.next();
                raft.last_receive_time = now;
                tokio::spawn(start_election(rf));
            }
        }

        time::sleep(time::Duration::from_millis(30)).await;
    }
}

async fn start_election(raft: Arc<Mutex<Raft>>) {
    let mut rxs = Vec::new();
    let num_of_peers;
    {
        let raft = Clone::clone(&raft);
        let mut rt = raft.lock().unwrap();
        num_of_peers = rt.peers.len();
        rt.convert_to_candidate();

        let args = rt.generate_request_vote_rpc_request();
        // let args = RequestVoteArgs {
        //     term: rt.current_term,
        //     candidate_id: rt.me as u64,
        //     last_log_index: rt.last_log_index(),
        //     last_log_term: rt.last_log_term(),
        // };

        for index in 0..num_of_peers {
            if index != rt.me {
                info!(
                    "[election]: candidate: {}\t follower: {}\t args: {:?}\n",
                    rt.me, index, args
                );
                let rx = rt.send_request_vote(index, args.clone());
                rxs.push(rx);
            }
        }
    }

    // 计票
    let done_cnt = Arc::new(AtomicUsize::new(0));
    let gain_vote_cnt = Arc::new(AtomicUsize::new(0));

    for rx in rxs {
        let dc = done_cnt.clone();
        let gvc = gain_vote_cnt.clone();
        let raft = Clone::clone(&raft);
        tokio::spawn(async move {
            if let Ok(Ok(reply)) = rx.await {
                info!("[election]: reply {:?}", reply);
                if reply.vote_granted {
                    gvc.fetch_add(1, Ordering::Relaxed);
                } else {
                    let mut raft = raft.lock().unwrap();
                    if reply.term > raft.current_term {
                        raft.convert_to_follower(reply.term);
                    }
                }
            }
            dc.fetch_add(1, Ordering::Relaxed)
        });
    }

    // 统计票数
    while done_cnt.load(Ordering::Relaxed) < num_of_peers {
        // 加上candidate 投票给自己
        if gain_vote_cnt.load(Ordering::Relaxed) >= num_of_peers / 2 {
            let raft = Clone::clone(&raft);
            let mut rt = raft.lock().unwrap();
            // info!("role: {:?}", rt.role);
            if rt.role == Role::Candidate {
                rt.covert_to_leader();
            }
            return;
        }
        time::sleep(time::Duration::from_millis(10)).await;
    }
}

async fn heartbeat(raft: Arc<Mutex<Raft>>, stop_signal: watch::Receiver<bool>) {
    while !*stop_signal.borrow() {
        {
            let rt = raft.lock().unwrap();
            if rt.role == Role::Leader {
                tokio::spawn(start_heartbeat(Clone::clone(&raft)));
            }
        }
        time::sleep(time::Duration::from_millis(100)).await;
    }
}

async fn start_heartbeat(raft: Arc<Mutex<Raft>>) {
    let mut rxs = Vec::new();
    {
        {
            let rt = raft.lock().unwrap();
            let num_of_peers = rt.peers.len();

            for peer in 0..num_of_peers {
                // index of log entry immediately preceding new ones
                let args = rt.generate_append_entries_rpc_request(peer);
                if peer != rt.me {
                    info!(
                        "[heartbeat].[tx]: leader: {}\t follower: {}\t args: {:?}\n",
                        rt.me, peer, args
                    );
                    let rx = rt.send_append_entries(peer, args.clone());
                    rxs.push(rx);
                }
            }
        }

        for rx in rxs {
            let raft = Clone::clone(&raft);
            tokio::spawn(async move {
                if let Ok(Ok(reply)) = rx.await {
                    let mut rt = raft.lock().unwrap();
                    info!(
                        "[heartbeat].[rx]: leader: {}, heartbeat reply: {:?}",
                        rt.me, reply
                    );
                    if rt.current_term < reply.term {
                        rt.convert_to_follower(reply.term);
                        return;
                    }
                }
            });
        }
    }
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    raft: Arc<Mutex<Raft>>,
    async_runtime: Arc<runtime::Runtime>,
    stop_signal_tx: Arc<watch::Sender<bool>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        // Node {
        //     raft: Arc::new(Mutex::new(raft)),
        // }
        let raft = Arc::new(Mutex::new(raft));
        let async_runtime = runtime::Runtime::new().unwrap();
        let (tx, rx) = watch::channel(false);

        async_runtime.spawn(leader_election(Clone::clone(&raft), Clone::clone(&rx)));
        async_runtime.spawn(heartbeat(Clone::clone(&raft), Clone::clone(&rx)));

        Node {
            raft,
            async_runtime: Arc::new(async_runtime),
            stop_signal_tx: Arc::new(tx),
        }
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        self.raft.lock().unwrap().start(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        // crate::your_code_here(())
        self.raft.lock().unwrap().current_term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        self.raft.lock().unwrap().role == Role::Leader
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
        }
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
        // info!("End server");
        self.stop_signal_tx.send(true).unwrap();
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        // Your code here (2A, 2B).
        let raft = Clone::clone(&self.raft);
        self.async_runtime
            .spawn(async move {
                let mut raft = raft.lock().unwrap();
                raft.request_vote(&args)
            })
            .await
            .unwrap()
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        let raft = Clone::clone(&self.raft);
        self.async_runtime
            .spawn(async move {
                let mut raft = raft.lock().unwrap();
                raft.append_entries(&args)
            })
            .await
            .unwrap()
    }
}
