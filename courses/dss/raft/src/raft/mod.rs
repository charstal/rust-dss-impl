use futures::channel::mpsc::UnboundedSender;
use futures::SinkExt;
use rand::{self, Rng};
use std::cmp::{max, min};
use std::fmt;
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

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
pub enum ApplyMsg {
    Command {
        data: Vec<u8>,
        index: u64,
    },
    // For 2D:
    Snapshot {
        data: Vec<u8>,
        term: u64,
        index: u64,
    },
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

#[derive(Message)]
struct PersistentState {
    #[prost(uint64, tag = "1")]
    term: u64,
    #[prost(uint64, tag = "2")]
    voted_for: u64,
    #[prost(message, tag = "3")]
    log: Option<Log>,
}
#[derive(Clone, Message)]
pub struct Log {
    #[prost(uint64, tag = "1")]
    last_included_index: u64,
    #[prost(uint64, tag = "2")]
    last_included_term: u64,
    #[prost(message, repeated, tag = "3")]
    entries: Vec<LogEntry>,
}

impl Log {
    pub fn new() -> Self {
        Log {
            entries: vec![LogEntry {
                term: 0,
                index: 0,
                command: vec![],
            }],
            last_included_index: 0,
            last_included_term: 0,
        }
    }

    pub fn truncate_after_log(&mut self, index: u64) {
        self.entries
            .drain(((index - self.last_included_index) as usize)..);
    }

    pub fn truncate_before_log(&mut self, index: u64) {
        // println!("{}, {}", index, self.last_included_index);
        self.entries
            .drain(..((index - self.last_included_index) as usize));
    }

    pub fn log_clone(&self) -> Vec<LogEntry> {
        self.entries.clone()
    }

    pub fn last(&self) -> Option<&LogEntry> {
        self.entries.last()
    }

    pub fn push(&mut self, log_entry: LogEntry) {
        self.entries.push(log_entry)
    }

    pub fn get(&self, index: u64) -> Option<&LogEntry> {
        // info!("get: {}, {}", index, self.last_included_index);
        self.entries
            .get((index - self.last_included_index) as usize)
    }

    pub fn find_conflict_entry(&self, term: u64) -> Option<&LogEntry> {
        self.entries.iter().find(|&x| x.term == term)
    }

    pub fn find_same_entry(&self, term: u64, index: u64) -> Option<&LogEntry> {
        self.entries
            .iter()
            .find(|&l| l.term == term && l.index == index)
    }

    pub fn set_last_entry_info(&mut self, last_included_index: u64, last_included_term: u64) {
        self.last_included_index = last_included_index;
        self.last_included_term = last_included_term;
    }

    pub fn log_clear(&mut self) {
        self.entries.drain(..);
    }

    pub fn log_len(&self) -> usize {
        self.entries
            .last()
            .map_or(self.last_included_index, |x| x.index) as usize
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
    logs: Log,
    // send newly committed messages
    apply_ch: UnboundedSender<ApplyMsg>,
    // time log
    last_receive_time: time::Instant,
    // snapshot
    snapshot: Option<Vec<u8>>,
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
            logs: Log::new(),
            apply_ch,
            last_receive_time: time::Instant::now(),
            snapshot: None,
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
        // info!("[persistent]: start persist");
        let data = self.persist_state();
        // labcodec::encode(&self.voted_for, &mut data).unwrap();
        // labcodec::encode(&self.logs, &mut data);
        self.persister.save_raft_state(data);
    }

    fn persist_state(&mut self) -> Vec<u8> {
        let ps = PersistentState {
            term: self.current_term,
            log: Some(self.logs.clone()),
            voted_for: self.voted_for.unwrap_or(u64::MAX as usize) as u64,
        };
        let mut data = vec![];
        labcodec::encode(&ps, &mut data).unwrap();
        data
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // info!("[persistent]: restore from persist");
        match labcodec::decode(data) {
            Ok(PersistentState {
                term,
                log,
                voted_for,
            }) => {
                self.current_term = term;
                self.logs = log.unwrap();
                self.voted_for = if voted_for == u64::MAX {
                    None
                } else {
                    Some(voted_for as usize)
                };
                self.last_applied = self.logs.last_included_index;
                // info!("last applied index: {}", self.logs.last_included_index);
            }
            Err(e) => {
                panic!("{:?}", e);
            }
        }
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

        let mut reply = RequestVoteReply {
            term: current_term,
            vote_granted: false,
        };

        if args.term < current_term {
            return Ok(reply);
        }

        if let Some(candidate_id) = self.voted_for {
            if candidate_id != args.candidate_id as usize {
                return Ok(reply);
            }
        }

        if self.is_last_log_up_to_date(args.last_log_term, args.last_log_index) {
            reply.vote_granted = true;
            self.voted_for = Some(args.candidate_id as usize);
            self.update_receiver_time();
        }

        self.persist();
        Ok(reply)
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

        let mut reply = AppendEntriesReply {
            term: self.current_term,
            success: false,
            conflict_index: 0,
            conflict_term: 0,
        };

        if self.current_term > args.term {
            return Ok(reply);
        }

        if self.current_term < args.term || self.role == Role::Candidate {
            self.convert_to_follower(args.term);
        }

        if args.prev_log_index > 0 {
            if let Some(log) = self.get_log_entry(args.prev_log_index) {
                if log.term != args.prev_log_term {
                    reply.conflict_term = log.term;
                    reply.conflict_index = self
                        .logs
                        .find_conflict_entry(log.term)
                        .map(|x| x.index)
                        .unwrap();
                    self.truncate_log(reply.conflict_index);
                    return Ok(reply);
                }
            } else {
                reply.conflict_index = self.last_log_index();
                reply.conflict_term = self.last_log_term();
                return Ok(reply);
            }
        }

        for entry in args.entries.iter() {
            if let Some(log) = self.get_log_entry(entry.index) {
                if log.term != entry.term {
                    self.truncate_log(log.index);
                    self.logs.push(entry.to_owned());
                }
            } else {
                self.logs.push(entry.to_owned());
            }
        }

        if args.leader_commit > self.commit_index {
            self.commit_index = min(args.leader_commit, self.last_log_index());
        }

        reply.success = true;
        self.persist();
        Ok(reply)
    }

    fn send_install_snapshot(
        &self,
        server: usize,
        args: InstallSnapshotArgs,
    ) -> oneshot::Receiver<Result<InstallSnapshotReply>> {
        let peer = &self.peers[server];
        let peer_clone = peer.clone();

        let (tx, rx) = oneshot::channel();
        peer.spawn(async move {
            let res = peer_clone.install_snapshot(&args).await.map_err(Error::Rpc);
            tx.send(res).unwrap_or(()); // 即使请求失败，也不能panic
        });
        rx
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
            let log_entry = LogEntry {
                term,
                index,
                command: buf,
            };
            self.logs.push(log_entry.clone());
            self.next_index[self.me] = index as u64 + 1;
            self.match_index[self.me] = index as u64;
            self.persist();
            info!("{:?}, start index {:?}", self, log_entry);
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
        self.persist();
    }

    fn convert_to_candidate(&mut self) {
        // set currentTerm = term, convert to follower
        self.role = Role::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.me);
        self.persist();
    }

    fn convert_to_leader(&mut self) {
        self.role = Role::Leader;
        for i in 0..self.peers.len() {
            self.next_index[i] = self.last_log_index() + 1;
        }
    }

    fn get_log_entry(&self, index: u64) -> Option<LogEntry> {
        match index {
            0 => None,
            idx => self.logs.get(idx).map(|x| x.to_owned()),
        }
    }

    fn last_log_index(&self) -> u64 {
        self.logs
            .last()
            .map_or(self.logs.last_included_index, |log| log.index)
    }

    fn last_log_term(&self) -> u64 {
        self.logs
            .last()
            .map_or(self.logs.last_included_term, |log| log.term)
    }

    fn is_last_log_up_to_date(&self, args_log_term: u64, args_log_index: u64) -> bool {
        let (last_log_term, last_log_index) =
            self.logs.last().map_or((0, 0), |log| (log.term, log.index));

        // info!("[args]: last_log_term: {}, last_log_index:{}\n\t[log]:last_log_term: {}, last_log_index:{}", args_log_term, args_log_index, last_log_term, last_log_index);
        // info!("server:{} logs:{:?}", self.me, self.logs);
        if args_log_term == last_log_term {
            args_log_index >= last_log_index
        } else {
            args_log_term > last_log_term
        }
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

        let next_index = self.next_index[peer];
        if let Some(prev_log) = self.get_log_entry(next_index - 1) {
            args.prev_log_index = prev_log.index;
            args.prev_log_term = prev_log.term;
        }

        for idx in next_index..=self.last_log_index() {
            let log = self.get_log_entry(idx).unwrap();
            args.entries.push(log);
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

    fn truncate_log(&mut self, index: u64) {
        self.logs.truncate_after_log(index);
    }

    fn update_commit_index(&mut self, commit_idx: u64) {
        if commit_idx <= self.commit_index || self.role != Role::Leader {
            return;
        }
        let peer_num = self.peers.len();
        let cnt = self
            .match_index
            .iter()
            .fold(0, |acc, x| acc + if *x >= commit_idx { 1 } else { 0 });

        if cnt <= peer_num / 2 {
            return;
        }
        if self.current_term != self.last_log_term() {
            return;
        }
        self.commit_index = commit_idx;
        // info!("[logs] {:?}", self.logs.entries);
        info!("{:?} commit to index {}", self, commit_idx);
    }

    fn cond_install_snapshot(
        &mut self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here (2D).
        if self.commit_index >= last_included_index {
            return false;
        }
        self.logs
            .set_last_entry_info(last_included_index, last_included_term);
        self.commit_index = last_included_index;
        self.last_applied = last_included_index;
        self.snapshot = Some(snapshot.to_vec());

        if let Some(_log_entry) = self
            .logs
            .find_same_entry(last_included_term, last_included_index)
        {
            self.logs.truncate_before_log(last_included_index);
        } else {
            self.logs.log_clear();
        }
        self.logs.push(LogEntry {
            term: last_included_term,
            index: last_included_index,
            command: vec![],
        });
        info!("{:?} after install snapshot", self);
        true
    }

    fn snapshot(&mut self, index: u64, snapshot: &[u8]) {
        // Your code here (2D).
        if let Some(entry) = self.get_log_entry(index) {
            // info!(
            //     "[snapshot]: start snapshot: from {} to {}",
            //     self.logs.last_included_index, index
            // );
            info!("{:?} snapshot at index {}", self, index);
            self.logs.truncate_before_log(index);
            self.logs.set_last_entry_info(entry.index, entry.term);
            self.snapshot = Some(snapshot.to_vec());
            let state = self.persist_state();
            self.persister
                .save_state_and_snapshot(state, snapshot.to_vec());
            // info!("after snapshot logs: {:?}", self.logs.entries);
        }
    }
}

impl fmt::Debug for Raft {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Raft({},term={},l=[{},{}],{:?},la:{},ci:{})",
            self.me,
            self.current_term,
            self.logs.last_included_index,
            self.logs.log_len(),
            self.role,
            self.last_applied,
            self.commit_index,
        )
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.cond_install_snapshot(0, 0, &[]);
        let _ = self.snapshot(0, &[]);
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
        let _ = &self.logs.last_included_term;
        let _ = &self.logs.last_included_index;
    }
}

async fn leader_election(raft: Arc<Mutex<Raft>>, stop_signal: watch::Receiver<bool>) {
    // info!("Start leader election");
    let mut timeout = ElectionTimeOut::new();
    while !*stop_signal.borrow() {
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

        for index in 0..num_of_peers {
            if index != rt.me {
                // info!(
                //     "[election]: candidate: {}\t follower: {}\t args: {:?}",
                //     rt.me, index, args
                // );
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
                // info!("[election]: reply {:?}", reply);
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
                rt.convert_to_leader();
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
            // info!("{}: {:?}", rt.me, rt.logs);
            if rt.role == Role::Leader {
                tokio::spawn(start_heartbeat(Clone::clone(&raft)));
            }
        }
        time::sleep(time::Duration::from_millis(100)).await;
    }
}

async fn start_heartbeat(raft: Arc<Mutex<Raft>>) {
    let mut rxs = Vec::new();
    let mut rxs_snapshot = Vec::new();
    {
        {
            let rt = raft.lock().unwrap();
            let num_of_peers = rt.peers.len();

            for peer in 0..num_of_peers {
                if peer != rt.me {
                    let prev_log_index = rt.next_index[peer] - 1;
                    if prev_log_index < rt.logs.last_included_index {
                        let args = InstallSnapshotArgs {
                            leader_id: rt.me as u64,
                            term: rt.current_term,
                            last_included_index: rt.logs.last_included_index,
                            last_included_term: rt.logs.last_included_term,
                            data: rt.snapshot.clone().unwrap(),
                        };
                        let rx = rt.send_install_snapshot(peer, args.clone());
                        rxs_snapshot.push((peer, args.clone(), rx));
                        info!(
                            "[install snapshot].[tx]: leader: {}\t follower: {}\t args: {:?}\n",
                            rt.me, peer, args
                        );
                    } else {
                        let args = rt.generate_append_entries_rpc_request(peer);
                        let rx = rt.send_append_entries(peer, args.clone());
                        rxs.push((peer, args.clone(), rx));
                        // info!(
                        //     "[heartbeat].[tx]: leader: {}\t follower: {}\t args: {:?}\n",
                        //     rt.me, peer, args
                        // );
                    }
                }
            }
        }

        for (peer, args, rx) in rxs {
            let raft = Clone::clone(&raft);
            tokio::spawn(async move {
                if let Ok(Ok(reply)) = rx.await {
                    let mut rt = raft.lock().unwrap();
                    // info!(
                    //     "[heartbeat].[rx] from:{}, leader: {}, heartbeat reply: {:?}",
                    //     peer, rt.me, reply
                    // );

                    if rt.current_term < reply.term {
                        rt.convert_to_follower(reply.term);
                        return;
                    }
                    if !reply.success {
                        // rt.next_index[peer] -= 1;
                        rt.next_index[peer] = min(
                            rt.next_index[peer],
                            rt.logs
                                .find_conflict_entry(reply.conflict_term)
                                .map_or(reply.conflict_index, |x| x.index),
                        );
                        rt.next_index[peer] = max(rt.next_index[peer], 1);
                        // info!(
                        //     "[next index]:{}, logs: {:?}",
                        //     rt.next_index[peer], rt.logs.entries
                        // );
                    } else {
                        let last = args.prev_log_index + args.entries.len() as u64;
                        rt.next_index[peer] = last + 1;
                        rt.match_index[peer] = last;
                        // info!(
                        //     "[raft].[index], next_index:{:?}, match_index:{:?}, commit_indx: {}",
                        //     rt.next_index, rt.match_index, rt.commit_index
                        // );
                        rt.update_commit_index(last);
                    }
                }
            });
        }

        for (peer, args, rx) in rxs_snapshot {
            let raft = Clone::clone(&raft);
            tokio::spawn(async move {
                if let Ok(Ok(reply)) = rx.await {
                    let mut rt = raft.lock().unwrap();
                    info!(
                        "[install snapshot].[rx] from:{}, leader: {}, heartbeat args: {:?}",
                        peer, rt.me, &args
                    );

                    if rt.current_term < reply.term {
                        rt.convert_to_follower(reply.term);
                        return;
                    }
                    rt.next_index[peer] = args.last_included_index + 1;
                    rt.match_index[peer] = args.last_included_index;
                }
            });
        }
    }
}

async fn apply_log(raft: Arc<Mutex<Raft>>, stop_signal: watch::Receiver<bool>) {
    let mut apply_ch = {
        let rf = raft.lock().unwrap();
        Clone::clone(&rf.apply_ch)
    };
    while !*stop_signal.borrow() {
        let mut logs = vec![];
        {
            let mut rt = raft.lock().unwrap();
            if rt.last_applied < rt.commit_index {
                for idx in rt.last_applied + 1..=rt.commit_index {
                    let log = rt.get_log_entry(idx).unwrap();
                    logs.push(log);
                }
                rt.last_applied = rt.commit_index;
            }
        }

        for log in logs {
            let am = ApplyMsg::Command {
                data: log.command,
                index: log.index,
            };

            let _ = apply_ch.send(am).await;
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
        async_runtime.spawn(apply_log(Clone::clone(&raft), Clone::clone(&rx)));

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

    /// A service wants to switch to snapshot.  
    ///
    /// Only do so if Raft hasn't have more recent info since it communicate
    /// the snapshot on `apply_ch`.
    pub fn cond_install_snapshot(
        &self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here.
        // Example:
        // self.raft.cond_install_snapshot(last_included_term, last_included_index, snapshot)
        self.raft.lock().unwrap().cond_install_snapshot(
            last_included_term,
            last_included_index,
            snapshot,
        )
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub fn snapshot(&self, index: u64, snapshot: &[u8]) {
        // Your code here.
        // Example:
        // self.raft.snapshot(index, snapshot)
        self.raft.lock().unwrap().snapshot(index, snapshot);
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

    async fn install_snapshot(
        &self,
        args: InstallSnapshotArgs,
    ) -> labrpc::Result<InstallSnapshotReply> {
        let raft = Clone::clone(&self.raft);
        self.async_runtime
            .spawn(async move {
                let mut apply_ch;
                let reply;
                {
                    let mut raft = raft.lock().unwrap();
                    apply_ch = Clone::clone(&raft.apply_ch);
                    reply = InstallSnapshotReply {
                        term: raft.current_term,
                    };

                    if args.term < raft.current_term {
                        return Ok(reply);
                    }

                    if args.term > raft.current_term {
                        raft.convert_to_follower(args.term);
                    }

                    if args.last_included_index <= raft.logs.last_included_index {
                        return Ok(reply);
                    }

                    raft.update_receiver_time();
                }

                apply_ch
                    .send(ApplyMsg::Snapshot {
                        data: args.data.clone(),
                        term: args.last_included_term,
                        index: args.last_included_index,
                    })
                    .await
                    .unwrap();

                Ok(reply)
            })
            .await
            .unwrap()
    }
}
