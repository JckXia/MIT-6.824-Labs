package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
//	"bytes"
	"sync"
	"time"
 
	"sync/atomic"
	//"util"
	//"fmt"
//	"6.824/labgob"
	"6.824/labrpc"
)
var HAS_NOT_VOTED int= -1
var LOG_LEVEL_ELECTION ="election"
var LOG_LEVEL_REPLICATION="log_replication"

const (
	Follower int = 0
	Candidate  	 = 1
	Leader		 = 2
)
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	leaderId int				  // Raft saves a copy of the leader state
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int 
	votedFor int
	log []Log

	commitIndex int
	lastApplied int

	nodeStatus int 
	votesReceived int

	// Election timeout info
	lastContactWithPeer time.Time
	electionTimeout int 
}

// Utility function. Caller should guarantee its threadsafe
func (rf * Raft) AppendNewLog(newLog Log) {
	rf.log = append(rf.log, newLog)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var isLeader = rf.nodeStatus == Leader
	var currTerm = rf.currentTerm
	DPrintf(LOG_LEVEL_ELECTION, "server %d term: %d ", rf.me, currTerm) 
	return currTerm, isLeader 
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type Log struct {
	CommandValid bool
	Command interface{}
	CommandTerm int 
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term int
	CandidateId int
	LastLogIndex int 
	LastLogTerm int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	 
	// Your code here (2A, 2B).
	rf.mu.Lock() // Need to ensure the data is consistent
	candidate_term := args.Term
	host_voted := rf.votedFor
	host_term := rf.currentTerm  

	DPrintf(LOG_LEVEL_ELECTION, "Host %d (%s, term: %d) recv RequestVoteRPC from (pid: %d, term: %d) ", rf.me, GetServerState(rf.nodeStatus), rf.currentTerm, args.CandidateId, candidate_term)
	rf.termCheck(candidate_term)
	reply.Term = host_term

	if candidate_term < host_term {
		reply.VoteGranted = false 
	} else {
 
		if rf.nodeStatus == Follower && (host_voted == HAS_NOT_VOTED || host_voted == args.CandidateId) && rf.candidateLogIsUpToDate(args.LastLogIndex, args.LastLogTerm, candidate_term) {
			 
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.lastContactWithPeer = time.Now()
		}
	}

	rf.mu.Unlock()
}

func (rf * Raft) candidateLogIsUpToDate(candidateLastLogIndex int, candidateLastLogTerm int, candidateTerm int) (bool) {
	return true;
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	
	rf.mu.Lock()
  	candidate_term := args.Term
	host_term := rf.currentTerm
 
	DPrintf(LOG_LEVEL_ELECTION, "Host %d (%s, term: %d) recv AppendEntriesRPC from (pid: %d, term: %d) ", rf.me, GetServerState(rf.nodeStatus), rf.currentTerm, args.LeaderId, candidate_term)
	
	check_result := rf.termCheck(candidate_term)
	reply.Term  = rf.currentTerm

	if check_result == false {
		rf.leaderId = args.LeaderId
	}

	if candidate_term < host_term {
	 
		reply.Success = false
	} else {
		rf.lastContactWithPeer = time.Now()
		rf.leaderId = args.LeaderId
		
		reply.Success = true
	}
 

	rf.mu.Unlock()
}

// What if we design an exponeial backoff wrt number of failed tries?
func (rf *Raft) getNewElectionTimeout() int {
	return RandRange(230,400)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	DPrintf(LOG_LEVEL_ELECTION, "Host %d (%s, term: %d) sent RequestVoteRPC to (pid: %d)", rf.me,  GetServerState(rf.nodeStatus), rf.currentTerm, server)
	rf.termCheck(reply.Term)
 

	if rf.nodeStatus == Candidate && reply.VoteGranted == true{
		rf.votesReceived++
	}
	 
	rf.lastContactWithPeer = time.Now()
	rf.mu.Unlock()
	
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
 
	rf.mu.Lock()
	 
	rf.termCheck(reply.Term)
 
	if reply.Success == true {
	   rf.lastContactWithPeer = time.Now()
	}
 
	rf.mu.Unlock()
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) lifeCycleManager() {
	for rf.killed() == false {

		rf.mu.Lock()
		now := time.Now()

		elapsed := now.Sub(rf.lastContactWithPeer)
 
		if rf.nodeStatus == Follower && (elapsed > (time.Duration(rf.electionTimeout) * time.Millisecond)) {
			rf.followerTransitionToCandidate()
		} 
		rf.mu.Unlock()
			
			
		rf.mu.Lock()
		
		if rf.nodeStatus == Candidate {
			if rf.votesReceived > (len(rf.peers)/2) {
	 
				rf.candidateTransitionToLeader()
			} else {
				// Check for election timeouts
				elaspedTime := time.Now().Sub(rf.lastContactWithPeer)
				if elaspedTime > (time.Duration(rf.electionTimeout) * time.Millisecond)  {
					rf.candidateStartElection()
				}
			}
		}
		rf.mu.Unlock()

		// Todo: Make this random
		// duration := 10 * time.Millisecond
		time.Sleep(5 * time.Millisecond)
	}
}

// 3 heart beats
// 1/3 seconds
// 10 heart beats a second 
func (rf * Raft) LeaderHeartBeatManager() {
	for rf.killed() == false {
		rf.mu.Lock()
		leaderId := rf.me 
		leaderTerm := rf.currentTerm

		if rf.nodeStatus == Leader {
			go rf.leaderSendHeartBeatMessages(leaderId, leaderTerm)
		}
		rf.mu.Unlock()

		time.Sleep(180 * time.Millisecond)
	}
}

// This may be where election happens
func (rf * Raft) followerTransitionToCandidate() {
	
	 
	//rf.currentTerm++
	//rf.votedFor = rf.me 
	//rf.lastContactWithPeer = time.Now()
	//rf.electionTimeout = rf.getNewElectionTimeout()
	rf.nodeStatus = Candidate
	// candidateTerm := rf.currentTerm
	// candidateId := rf.me 
	DPrintf(LOG_LEVEL_ELECTION,"Server id %d became candidate for term %d ", rf.me, rf.currentTerm)

	rf.candidateStartElection()
}

func (rf * Raft) candidateStartElection() {
 
	 
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.votesReceived = 1
	rf.lastContactWithPeer = time.Now()
	rf.electionTimeout = rf.getNewElectionTimeout()
	requestVoteArgs := RequestVoteArgs{rf.currentTerm, rf.me, 0,0} 
	DPrintf(LOG_LEVEL_ELECTION,"Candidate id %d is starting an election for term %d ", rf.me, rf.currentTerm)

	for peerId := range rf.peers {
		if peerId != rf.me {
			go rf.sendRequestVote(peerId,&requestVoteArgs, &RequestVoteReply{})
		}
	}
}

// func (rf *Raft) candidateTransitionToFollower() {

// }

func (rf * Raft) leaderSendHeartBeatMessages(leaderId int, leaderTerm int) {
	// leaderTerm := rf.currentTerm
	// leaderId := rf.me 

	appendEntriesArgs := AppendEntriesArgs{leaderTerm, leaderId, 0,0,nil, 0}

	for peerId := range rf.peers {
		if peerId != leaderId {
			go rf.sendAppendEntries(peerId, &appendEntriesArgs, &AppendEntriesReply{})
		}
	}
}

func (rf * Raft) candidateTransitionToLeader() {
	rf.leaderId = rf.me 
	rf.nodeStatus = Leader
	 
	DPrintf(LOG_LEVEL_ELECTION, "Server id %d  became leader for term %d ", rf.me , rf.currentTerm)

	rf.leaderSendHeartBeatMessages(rf.me, rf.currentTerm)
}

func (rf * Raft) leaderTransitionToFollower() {

}

func (rf *Raft) setStateToFollower(peerTerm int) {
	rf.currentTerm = peerTerm
	rf.votedFor = HAS_NOT_VOTED
	rf.votesReceived = 0 
	rf.nodeStatus = Follower
}
//  Check Used for all RPC request/response
//	 -> If discovered peer with higher term, set self
//		to follower
//	-> semantics: host have higher/equivalent term than candidate
func (rf * Raft) termCheck(peerTerm int) (bool) {
	host_term := rf.currentTerm
	if peerTerm > host_term {
		rf.setStateToFollower(peerTerm)
		return false 
	}
	return true
}

func (rf * Raft) bootStrapState(hostServerId int) {
	rf.me = hostServerId
	rf.votedFor = HAS_NOT_VOTED
	rf.leaderId = HAS_NOT_VOTED
	// Raft logs start at 1 and not 0. Stub out a log
	rf.AppendNewLog(Log{true, "ok", -1})
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nodeStatus = Follower
	rf.votesReceived = 0
	
	rf.lastContactWithPeer = time.Now()
	rf.electionTimeout = rf.getNewElectionTimeout()
}
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister

	rf.bootStrapState(me)
	
	// 2A initialization code
		
	// 2B initialization code

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.lifeCycleManager()
    go rf.LeaderHeartBeatManager()

	return rf
}
