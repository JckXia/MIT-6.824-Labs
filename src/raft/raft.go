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
	"bytes"
	"sync"
	"time"
 
	"sync/atomic"
	//"util"
	// "fmt"
	"6.824/labgob"
	"6.824/labrpc"
)
var HAS_NOT_VOTED int= -1
var LOG_LEVEL_ELECTION ="election"
var LOG_LEVEL_REPLICATION="log_replication"
var LOG_LEVEL_PERSISTENCE="log_persistence"
var LOG_LEVEL_WARN="warn"

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
	logs []Log

	commitIndex int
	lastApplied int

	nodeStatus int 
	votesReceived int

	// Election timeout info
	lastContactWithPeer time.Time
	electionTimeout int

	applyCh chan ApplyMsg

	nextIndex  map[int]int 
	matchIndex map[int]int

	// Reserved for leader to keep track of its commitIndex
	leaderCommitWaterMark int
	followerReplicationCount int // Once this value > 50% we reset leader commitIndex
}

// Utility function. Caller should guarantee its threadsafe
func (rf * Raft) AppendNewLog(newLog Log) {
	rf.logs = append(rf.logs, newLog)
 
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var isLeader = rf.nodeStatus == Leader
	var currTerm = rf.currentTerm
	DPrintf(LOG_LEVEL_ELECTION, "server %d term: %d , isLeader: %t", rf.me, currTerm, isLeader) 
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
	 w := new(bytes.Buffer)
	 e := labgob.NewEncoder(w)

 
	 e.Encode(rf.votedFor)
	 e.Encode(rf.currentTerm)
	 e.Encode(rf.logs)
	 data := w.Bytes()
	 rf.persister.SaveRaftState(data)
	 
	 //DPrintf(LOG_LEVEL_PERSISTENCE,"(Leader %d) Host %d persist state: (votedFor: %d, currentTerm: %d, lastLogIdx: %d, lastLogTerm %d)",rf.leaderId, rf.me, rf.votedFor, rf.currentTerm, rf.getLastLogIdx(), rf.getLastLogTerm())
	 DebugP(dPersist, "S%d,V:%d,CT:%d,logs:[%s]", rf.me, rf.votedFor, rf.currentTerm, serializeLogContents(rf.logs))
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
	 r := bytes.NewBuffer(data)
	 d := labgob.NewDecoder(r)
	 
	 var votedFor int 
	 var currentTerm int
	 var raftLogs []Log
	 if d.Decode(&votedFor) != nil || 
	 	d.Decode(&currentTerm) != nil ||
		d.Decode(&raftLogs) != nil {
		
		DebugPrintf(LOG_LEVEL_WARN,"Could not deserialize from data")
	 } else {
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.logs = raftLogs
		DPrintf(LOG_LEVEL_PERSISTENCE,"Server %d votedfor %d currentTerm %d, log len %d", rf.me, rf.votedFor, rf.currentTerm, len(rf.logs))

	 }
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// ************************* LOG OPERATIONS ***************// 
func (rf *Raft) appendLogEntry(command interface{}){
	newLog := Log{true, command, rf.currentTerm}
	rf.logs = append(rf.logs, newLog)
	rf.persist()
}

func (rf *Raft) deleteLogSuffix(startLogIdx int) {
	if startLogIdx <= 0 || startLogIdx >= len(rf.logs) {
		DPrintf(LOG_LEVEL_WARN, "Warning! attempting to delete log out of bound at idx: %d", startLogIdx)	
	}

	if startLogIdx < len(rf.logs) {
		rf.logs = append(rf.logs[:startLogIdx])
		rf.persist()
	}
}

// At any given moment, there will be at least one entry in rf.logs
//	because raft logs are index'd by 1
func (rf *Raft) getLastLogIdx() int {
	return len(rf.logs) -1
}

func (rf *Raft) getLastLog() Log {
	lastLogIdx := rf.getLastLogIdx()
	return rf.logs[lastLogIdx]
}

func (rf *Raft) getLastLogTerm() (int) {
	log := rf.getLastLog()
	return log.CommandTerm
}

// Retrieve all logs starting at some log index
func (rf *Raft) getLeaderLogs(startLogIdx int) []Log {
	if startLogIdx >= rf.getLastLogIdx() + 1 {
		DPrintf(LOG_LEVEL_WARN, "Warning! attempting to retrieve log out of bound at idx: %d", startLogIdx)
	}
	logsToReturn := rf.logs[startLogIdx:]

	newLogs := make([]Log, len(logsToReturn))
	
	for i :=0; i <len(logsToReturn); i++ {
		newLogs[i].CommandTerm = logsToReturn[i].CommandTerm
		newLogs[i].Command = logsToReturn[i].Command 
		newLogs[i].CommandValid = logsToReturn[i].CommandValid
	}

	return newLogs
}

// Converge logs from leader
func (rf *Raft) acceptLogsFromLeader(leaderLogs *[]Log, startLogIdx int) {
	logsFromLeader := *leaderLogs
	startIdx := 0

	for hostLogIdxStart := startLogIdx; hostLogIdxStart < len(rf.logs); hostLogIdxStart++ {
		// Logs with conflicting term found!
		if startIdx < len(logsFromLeader) && rf.logs[hostLogIdxStart].CommandTerm != logsFromLeader[startIdx].CommandTerm {
			rf.logs = rf.logs[:hostLogIdxStart]
			rf.logs = append(rf.logs, logsFromLeader[startIdx])
		}
		startIdx++
	}

	for ;startIdx < len(logsFromLeader); startIdx++ {
		rf.logs = append(rf.logs, logsFromLeader[startIdx])
	}
	rf.persist()
}

func (rf * Raft) printLogContent() {
	for i := 0; i < len(rf.logs); i++ {
		DPrintf(LOG_LEVEL_WARN, "Host: %d Content: %s, Term: %d ", rf.me, rf.logs[i].Command, rf.logs[i].CommandTerm)
	}
}

// ********************************************************//


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
	LogConsistent bool
	Success bool

	// Optimization
	Xterm int
	XIndex int 
	Xlen int
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
	host_term := rf.currentTerm  

	DPrintf(LOG_LEVEL_ELECTION, "Host %d (%s, term: %d) recv RequestVoteRPC from (pid: %d, term: %d) ", rf.me, GetServerState(rf.nodeStatus), rf.currentTerm, args.CandidateId, candidate_term)
	rf.termCheck(candidate_term)
	reply.Term = host_term

	if candidate_term < host_term {
		reply.VoteGranted = false 
	} else {
 
		if rf.nodeStatus == Follower && (rf.votedFor == HAS_NOT_VOTED || rf.votedFor == args.CandidateId) && rf.candidateLogIsUpToDate(args.LastLogIndex, args.LastLogTerm, candidate_term) {
			 
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.lastContactWithPeer = time.Now()
			rf.persist()
			DPrintf(LOG_LEVEL_ELECTION, "Host %d granted vote for candidate %d on term %d", rf.me, rf.votedFor, candidate_term)
		}
	}

	rf.mu.Unlock()
}


func (rf * Raft) candidateLogIsUpToDate(candidateLastLogIndex int, candidateLastLogTerm int, candidateTerm int) (bool) {
	hostLastLogTerm := rf.getLastLogTerm()
	hostLastLogIdx := rf.getLastLogIdx()

	if candidateLastLogTerm < hostLastLogTerm {
		return false
	}

	if candidateLastLogTerm > hostLastLogTerm {
		return true
	}

	if candidateLastLogTerm == hostLastLogTerm {
		if candidateLastLogIndex < hostLastLogIdx {
			return false
		}
	}

	return true;
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	
	rf.mu.Lock()
  	candidate_term := args.Term
	host_term := rf.currentTerm
 
	DPrintf(LOG_LEVEL_ELECTION, "Host %d (%s, term: %d) recv AppendEntriesRPC from (pid: %d, term: %d) ", rf.me, GetServerState(rf.nodeStatus), rf.currentTerm, args.LeaderId, candidate_term)
	
	check_result := rf.termCheck(candidate_term)
	reply.Term  = rf.currentTerm
	reply.LogConsistent = true
	reply.Xterm = -1
	reply.XIndex = -1
	reply.Xlen = -1

	if candidate_term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
	}
	
	if check_result == false {
		rf.leaderId = args.LeaderId
	}

	if candidate_term < host_term {
		reply.Success = false

	} else if rf.serverContainsLeaderLog(args.PrevLogIndex, args.PrevLogTerm) == false {
	//	DPrintf(LOG_LEVEL_REPLICATION, "Host %d does not contain log %d from leader %d", rf.me, args.PrevLogIndex, args.LeaderId)
		rf.lastContactWithPeer = time.Now()
		rf.leaderId = args.LeaderId
		reply.Success = false
		reply.LogConsistent = false

		// log is too short. 
		if rf.getLastLogIdx() < args.PrevLogIndex {
			reply.Xlen = len(rf.logs)
		} else {
			conflict_term := rf.logs[args.PrevLogIndex].CommandTerm
			first_idx_with_conflict := rf.lookupFirstEntryWithTerm(conflict_term)

			reply.Xterm = conflict_term
			reply.XIndex = first_idx_with_conflict
		}

	} else if rf.serverContainsLeaderLog(args.PrevLogIndex, args.PrevLogTerm) == true {
	//	DPrintf(LOG_LEVEL_REPLICATION, "Host %d contain log %d from leader %d", rf.me, args.PrevLogIndex, args.LeaderId)
		rf.lastContactWithPeer = time.Now()
		rf.leaderId = args.LeaderId
		
		reply.LogConsistent = true
		reply.Success = true

		rf.acceptLogsFromLeader(&args.Entries, args.PrevLogIndex + 1)
 
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIdx())
		}

	} else {
		rf.lastContactWithPeer = time.Now()
		rf.leaderId = args.LeaderId
		
		reply.Success = true
	}
 
	rf.mu.Unlock()
}

// Implements step 2 of AppendEntries
//	-> Semantics: If log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
func (rf *Raft) serverContainsLeaderLog(leaderPrevLogIdx int, leaderPrevLogTerm int) bool {
	 
	if rf.getLastLogIdx() < leaderPrevLogIdx {
		return false
	}

	if rf.logs[leaderPrevLogIdx].CommandTerm != leaderPrevLogTerm {
		return false	
	}

	return true
}


// TODO:
//	-> Rewrite this using binary search
func (rf *Raft) lookupFirstEntryWithTerm(xTerm int) int {
	for i:= 0; i< len(rf.logs); i++ {
		if rf.logs[i].CommandTerm == xTerm {
			return i
		}
	}
	return -1
}

// Reserved for leader
func (rf * Raft) lookupLastEntryWithTerm(xTerm int) int {


	for i := len(rf.logs) - 1; i >=0; i-- {
		if rf.logs[i].CommandTerm == xTerm {
			return i
		}
	}
	return -1
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
	
	if !ok {
		DPrintf(LOG_LEVEL_ELECTION,"Servers %d is unreachable", server)
		return ok
	}

	rf.mu.Lock()
	DPrintf(LOG_LEVEL_ELECTION, "Host %d (%s, term: %d) sent RequestVoteRPC to (pid: %d)", rf.me,  GetServerState(rf.nodeStatus), rf.currentTerm, server)
	
	
	if rf.currentTerm != args.Term {
		
		rf.mu.Unlock()
		return false
	}

	rf.termCheck(reply.Term)

	if rf.nodeStatus == Candidate && reply.VoteGranted == true{
		rf.votesReceived++
	}
 	 
	rf.lastContactWithPeer = time.Now()
	rf.mu.Unlock()
	
	return ok
}


func (rf *Raft) scanNextIndex() {
	leaderLastLogIdx := rf.getLastLogIdx()
	for peerId := range rf.peers {
		if peerId != rf.me && leaderLastLogIdx >= rf.nextIndex[peerId] {
			DPrintf(LOG_LEVEL_REPLICATION, "Leader %d sending log to peer %d", rf.me, peerId )
			//go send
			prevLogIdx := rf.nextIndex[peerId] - 1
			prevLogTerm := rf.logs[prevLogIdx].CommandTerm
			entries := rf.getLeaderLogs(rf.nextIndex[peerId])
			initalAppendEntryRPC := AppendEntriesArgs{rf.currentTerm, rf.me, prevLogIdx, prevLogTerm, entries, rf.commitIndex}
			go rf.sendAppendEntries(peerId, &initalAppendEntryRPC, &AppendEntriesReply{})
		}
	}
}


func (rf * Raft) lookForMatchIndex() {
	for rf.killed() == false {
		rf.mu.Lock()
			if rf.nodeStatus == Leader {
				for peerId := range rf.peers {
					if peerId != rf.me && rf.getLastLogIdx() >= rf.nextIndex[peerId] {
						prevLogIdx := rf.nextIndex[peerId] - 1
						prevLogTerm := rf.logs[prevLogIdx].CommandTerm
						entries := rf.getLeaderLogs(rf.nextIndex[peerId])
						initalAppendEntryRPC := AppendEntriesArgs{rf.currentTerm, rf.me, prevLogIdx, prevLogTerm, entries, rf.commitIndex}
						go rf.sendAppendEntries(peerId, &initalAppendEntryRPC, &AppendEntriesReply{})
					}
				}
			}
		rf.mu.Unlock()
 
		
		time.Sleep(20* time.Millisecond)
	}
}
// Idea:
//	Make this a built-in mechanism for sending entries
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	for {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		DPrintf(LOG_LEVEL_ELECTION,"Servers %d is unreachable", server)
		return ok
	}
	rf.mu.Lock()

	// Handles cases where we get old RPC reply
 
	if rf.currentTerm != args.Term {
		rf.mu.Unlock()
		return false
	}

	host_term := args.Term
	if reply.Term > host_term {
		DPrintf(LOG_LEVEL_ELECTION, "Server %d is no longer the leader ", rf.me)
		rf.setStateToFollower(reply.Term)
		rf.mu.Unlock()
		return true;
	}

	if rf.nodeStatus != Leader || rf.currentTerm != args.Term {
		rf.mu.Unlock()
		return false
	}

	if reply.Success == true {
 
		rf.lastContactWithPeer = time.Now()
 	   
		newMatchIndex := args.PrevLogIndex + len(args.Entries) 

		rf.matchIndex[server] = newMatchIndex
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	//DebugPrintf("Leader %d to host %d, (nextIndex %d, matchIndex %d) log len %d ", rf.me, server, rf.nextIndex[server], rf.matchIndex[server], len(args.Entries))
	   
	  //DPrintf(LOG_LEVEL_REPLICATION,"Match value peer %d match up to  %d", server, rf.matchIndex[server])
	   for N := rf.commitIndex + 1; N <= rf.getLastLogIdx(); N++ {
		 
			serverReplicatedCount := 0
			for peerId := range rf.peers {
				if peerId != rf.me && rf.matchIndex[peerId] >= N {
					serverReplicatedCount++
				}
				// DPrintf(LOG_LEVEL_REPLICATION, "replicated to %d servers", serverReplicatedCount)
				 
				if serverReplicatedCount >= (len(rf.peers)/2) {
				//	DPrintf(LOG_LEVEL_REPLICATION, "Server replicated count %d ", serverReplicatedCount)
					if rf.logs[N].CommandTerm == args.Term {
						rf.commitIndex = N
				//		DPrintf(LOG_LEVEL_REPLICATION, "Leader Set commit index to %d", N)
					}
				}
			}
	   }
	   rf.mu.Unlock()
	   return true	
	} else if reply.LogConsistent == false && rf.nodeStatus == Leader {
	//	DPrintf(LOG_LEVEL_WARN, "Retrying happened~! %d ", args.PrevLogIndex)
	//	DebugPrintf("Log inconsistent, retry")
		if reply.Xterm != -1 {
			// handles the first two cases
			lookupIdx := rf.lookupLastEntryWithTerm(reply.Xterm)
			
			if lookupIdx == -1 {
				// Leader does not have xTerm
				rf.nextIndex[server] = reply.XIndex
			} else {
				// Leader have xTerm
				rf.nextIndex[server] = lookupIdx + 1
			}

		} else if reply.Xlen != -1 {
			// handles the case where follower's log is too short
			//DebugPrintf(LOG_LEVEL_PERSISTENCE, "Hi")
			rf.nextIndex[server] = reply.Xlen
		} else {
			DebugPrintf("SHOULD ENTER THIS CASE!")
		}
		
		// rf.nextIndex[server]--
 		args.Term = rf.currentTerm
		args.PrevLogIndex = rf.nextIndex[server] - 1
		args.PrevLogTerm = rf.logs[args.PrevLogIndex].CommandTerm
		args.Entries = rf.getLeaderLogs(rf.nextIndex[server])
		rf.mu.Unlock()
		
	} else {
		// replied failed either
		//	-> reply.Success = false, failed due to server aggrements
		rf.mu.Unlock()
		return false
	} 

	time.Sleep(2* time.Millisecond)
	}
	return true
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := rf.nodeStatus == Leader
	if isLeader == false {
		return index, term, isLeader
	}

	// Client have reached the leader instance
	rf.appendLogEntry(command)
	index = rf.getLastLogIdx()
	term = rf.getLastLogTerm()
	DPrintf(LOG_LEVEL_REPLICATION,"Append command %s", command)
	// Use this as the opportunity to start replicating logs
	rf.scanNextIndex()
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
		 	
		if rf.commitIndex > rf.lastApplied {
			
			for rf.lastApplied < rf.commitIndex {
				rf.lastApplied++
				logEntryToCommit := rf.logs[rf.lastApplied]
				applyMsg := ApplyMsg{true, logEntryToCommit.Command, rf.lastApplied, true,nil, 0,0}	
				DebugP(dCommit, "S%d applying log (%s) at term %d, CI %d, isLeader: %v", rf.me, serializeLog(logEntryToCommit) , rf.currentTerm, rf.lastApplied, rf.nodeStatus == Leader)
				rf.applyCh <- applyMsg
			} 
		}

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
			rf.leaderSendHeartBeatMessages(leaderId, leaderTerm, &rf.nextIndex,&rf.logs)
		}
		rf.mu.Unlock()

		time.Sleep(200 * time.Millisecond)
	}
}

// This may be where election happens
func (rf * Raft) followerTransitionToCandidate() {
	
	rf.nodeStatus = Candidate
	DebugP(dElection, "S%d became candidate for term %d", rf.me, rf.currentTerm)
	rf.candidateStartElection()
}

func (rf * Raft) candidateStartElection() {
 
	 
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.votesReceived = 1
	rf.lastContactWithPeer = time.Now()
	rf.electionTimeout = rf.getNewElectionTimeout()
	rf.persist()
	DebugP(dElection,"C%d increments currentTerm to %d", rf.me, rf.currentTerm)
	
	requestVoteArgs := RequestVoteArgs{rf.currentTerm, rf.me, rf.getLastLogIdx(), rf.getLastLogTerm()} 
	for peerId := range rf.peers {
		if peerId != rf.me {
			DebugP(dElection, "C%d request vote from %d at term: %d", rf.me, peerId, rf.currentTerm)
			go rf.sendRequestVote(peerId,&requestVoteArgs, &RequestVoteReply{})
		}
	}
}
 

// What if we bundle up heart beat message with an actual AppendEntries RPC?
func (rf * Raft) leaderSendHeartBeatMessages(leaderId int, leaderTerm int, nextIndex *map[int]int, logs *[]Log) {

	emptyEntries := make([]Log,0)
 
	nextIndexArr := *nextIndex
	logArr := *logs
	DebugP(dElection,"L%d broadcast HB for term %d", rf.me, rf.currentTerm)
	for peerId := range rf.peers {
		if peerId != leaderId {
			prevLogIdx := nextIndexArr[peerId] - 1
			prevLogTerm := logArr[prevLogIdx].CommandTerm
			appendEntriesArgs := AppendEntriesArgs{leaderTerm, leaderId, prevLogIdx, prevLogTerm, emptyEntries, rf.commitIndex}
			go rf.sendAppendEntries(peerId, &appendEntriesArgs, &AppendEntriesReply{})
		}
	}
}

func (rf * Raft) candidateTransitionToLeader() {
	rf.leaderId = rf.me 
	rf.nodeStatus = Leader
	 
	DPrintf(LOG_LEVEL_ELECTION, "Server id %d  became leader for term %d ", rf.me , rf.currentTerm)
	
	leaderLastLogIdx := rf.getLastLogIdx() + 1
	DPrintf(LOG_LEVEL_PERSISTENCE,"Iniit all next index to %d ", leaderLastLogIdx)
	for peerId := range rf.peers {
		if peerId != rf.me {
			rf.nextIndex[peerId] = leaderLastLogIdx
			rf.matchIndex[peerId] = 0
		}
	}
	 
	rf.leaderSendHeartBeatMessages(rf.me, rf.currentTerm, &rf.nextIndex,&rf.logs)
}

func (rf * Raft) leaderTransitionToFollower() {

}

func (rf *Raft) setStateToFollower(peerTerm int) {
	rf.currentTerm = peerTerm
	rf.votedFor = HAS_NOT_VOTED
	rf.votesReceived = 0 
	rf.nodeStatus = Follower
	rf.persist()
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
	rf.AppendNewLog(Log{true, "Stub", 0})
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nodeStatus = Follower
	rf.votesReceived = 0
	
	rf.lastContactWithPeer = time.Now()
	rf.electionTimeout = rf.getNewElectionTimeout()
	
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)

	rf.leaderCommitWaterMark = 0
	// rf.persist()
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
	rf.applyCh = applyCh
	
	rf.bootStrapState(me)

	// 2A initialization code
		
	// 2B initialization code

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.lifeCycleManager()
    go rf.LeaderHeartBeatManager()
	go rf.lookForMatchIndex()

	return rf
}
