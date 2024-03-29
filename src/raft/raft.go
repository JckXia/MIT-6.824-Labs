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
//	"math/rand"
	"fmt"
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

const (
	LOG_TRUNCATED = -2
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

	//SnapShot
	lastIncludedIdx int // The index of the last entry in the log that the snapshot replaces (and state machine has applied)
	lastIncludedTerm int // The term of afforementioned index
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

func (rf *Raft) getRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)


	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.logs)
	e.Encode(rf.lastApplied)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastIncludedIdx)
	e.Encode(rf.lastIncludedTerm)

	return w.Bytes()
}
//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {

	 raftstate := rf.getRaftState()
	 rf.persister.SaveRaftState(raftstate)
	 
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
	 var lastApplied int
	 var commitIndex int 
	 var lastIncludedIdx int
	 var lastIncludedTerm int
	 if d.Decode(&votedFor) != nil || 
	 	d.Decode(&currentTerm) != nil ||
		d.Decode(&raftLogs) != nil || 
		d.Decode(&lastApplied) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&lastIncludedIdx) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		
		DebugPrintf(LOG_LEVEL_WARN,"Could not deserialize from data")
	 } else {
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.logs = raftLogs
		rf.commitIndex = commitIndex
		rf.lastApplied = lastApplied
		rf.lastIncludedIdx = lastIncludedIdx
		rf.lastIncludedTerm = lastIncludedTerm
 
		DPrintf(LOG_LEVEL_PERSISTENCE,"S erver %d votedfor %d currentTerm %d, log len %d", rf.me, rf.votedFor, rf.currentTerm, len(rf.logs))

	 }
}


func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply)bool{
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		return ok
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	DebugP(dCommit, "S%d attempts to send an snapshot request (lastInclIdx: %d, lastInclTerm: %d) to %d", rf.me, rf.lastIncludedIdx, rf.lastIncludedTerm, server)
	if rf.currentTerm != args.Term {
		return false;
	}

	host_term := args.Term
	if reply.Term > host_term {
		DebugP(dElection, "S%d steps down for term %d", host_term)
		rf.setStateToFollower(reply.Term)
		return true;
	}
	
	rf.lastContactWithPeer = time.Now()
	DebugP(dSnap, "S%d (isLeader=%t) received InstallSnapshot(LI: %d, LT: %d) response from %d",rf.me, rf.nodeStatus == Leader, args.LastIncludedIndex, args.LastIncludedTerm, server)

	if rf.nodeStatus == Leader && reply.Success == true { 
		rf.matchIndex[server] = args.LastIncludedIndex
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		DebugP(dSnap,"S%d has been brought up to %d", server, rf.lastIncludedIdx)

		DebugP(dCommit,"S%d has been brought up to speed on %d", server, rf.lastIncludedIdx)
		DebugP(dCommit,"S%d.matchIndex[S%d]=%d", rf.me, server, rf.matchIndex[server])
	} else if rf.nodeStatus == Leader {
		// Handle the OTHER case
		DebugP(dSnap, "S%d (leader) InstallSnapshot failed, check for third case",rf.me)
		if reply.PeerLastIncludedIdx <= rf.getLastLogIdx() {
			if rf.getLogTermAtIndex(reply.PeerLastIncludedIdx) == reply.PeerLastIncludedTerm {
				rf.matchIndex[server] = reply.PeerLastIncludedIdx
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				DebugP(dSnap,"(leader) S%d updated S%d matchindex to %d", rf.me, server, rf.matchIndex[server])
			}
		}
	}
	
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	
	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.setStateToFollower(args.Term)
		rf.leaderId = args.LeaderId
	}

	// Don't mess with logs if am leader
	if rf.nodeStatus == Leader {
		rf.mu.Unlock()
		return
	}
	
	DebugP(dCommit,"S%d received InstallSnapshotRPC (LA: %d, LT: %d) request from %d", rf.me,
	 args.LastIncludedIndex, args.LastIncludedTerm, args.LeaderId)
	DebugP(dCommit,"S%d current state: (%s)", rf.me, rf.printLogInformation()) 
	rf.lastContactWithPeer = time.Now()
	
	// We have already snapshotted, but respond to update caller state
	if args.LastIncludedIndex == rf.lastIncludedIdx && args.LastIncludedTerm == rf.lastIncludedTerm {
		DebugP(dSnap,"S%d already has lastIncludedIdx %d, return Success", rf.me, rf.lastIncludedIdx)

		reply.Success =true
		rf.mu.Unlock()
		return
	}

	// The server was lagging behind
	//	-> Erase the entire thing
	//	-> TODO: Refactor
	if rf.getLastLogIdx() < args.LastIncludedIndex {
 
		rf.lastIncludedIdx = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.lastApplied = args.LastIncludedIndex
		rf.commitIndex = rf.lastApplied

		rf.logs = []Log{Log{true, string(args.Data), args.LastIncludedTerm}}
		snapshotApplyMsg := ApplyMsg{false,"", 0, true, args.Data, args.LastIncludedTerm, args.LastIncludedIndex}
		DebugP(dSnap,"S%d is lagging behind S%d, fast forward to %s, return true", rf.me, args.LeaderId, rf.lastIncludedIdx)
		rf.persister.SaveStateAndSnapshot(rf.getRaftState(), args.Data)
		reply.Success = true
		rf.mu.Unlock()
		rf.applyCh <- snapshotApplyMsg
		
		return
	}
	/**
		if existing log entry has same index/term as snapshot's last included entry, retain log entries 
		following it
		-> Discard entire log
	**/
	if rf.getLastLogIdx() >= args.LastIncludedIndex  && args.LastIncludedIndex > rf.lastIncludedIdx {
	//	DebugP(dSnap, "S%d contains lastIncludedIdex %d, term: %d , inclTerm: %d", rf.me, args.LastIncludedIndex, rf.getLogTermAtIndex(args.LastIncludedIndex), args.LastIncludedTerm)
		if rf.getLogTermAtIndex(args.LastIncludedIndex) == args.LastIncludedTerm {
			rf.trimLogAt(args.LastIncludedIndex)
 
			DebugP(dSnap,"S%d is installing snapshot at %d from %d, S%d.logs [%s]", rf.me, args.LastIncludedIndex, args.LeaderId, rf.me,rf.printLogInformation())
				
			snapshotApplyMsg := ApplyMsg{false,"", 0, true, args.Data, args.LastIncludedTerm, args.LastIncludedIndex}
			reply.Success = true
			rf.lastApplied = args.LastIncludedIndex
			rf.commitIndex = rf.lastApplied
 
			DebugP(dLast,"S%d discarding log, reset lastApplied to %d", rf.me, rf.lastApplied)
			rf.persister.SaveStateAndSnapshot(rf.getRaftState(), args.Data)
			rf.mu.Unlock()
			rf.applyCh <- snapshotApplyMsg			
		} else {
			// Hit this case because the lastIncludedIdx != args.LastIncludedTerm. Doesn't matter. Discard log
			
			DebugP(dSnap,"S%d at idx %d is term %d, want %d", rf.me, args.LastIncludedIndex, rf.getLogTermAtIndex(args.LastIncludedIndex), args.LastIncludedTerm)
			DebugP(dSnap, "S%d log: %s", rf.me, rf.printLogInformation())

			rf.logs = []Log{Log{true, string(args.Data), args.LastIncludedTerm}}
			rf.lastApplied = args.LastIncludedIndex
			rf.commitIndex = rf.lastApplied
			
			rf.lastIncludedIdx = args.LastIncludedIndex
			rf.lastIncludedTerm = args.LastIncludedTerm

			snapshotApplyMsg := ApplyMsg{false,"",0,true, args.Data, args.LastIncludedTerm, args.LastIncludedIndex}

			reply.Success = true
			rf.persister.SaveStateAndSnapshot(rf.getRaftState(), args.Data) 
			rf.mu.Unlock()
			rf.applyCh <- snapshotApplyMsg
		}
		
	} else {
		// Pretty ugly. But have to do this cause at the moment, the implementation implies a stop-the-world
		// operation. Thus we'd have to update the matchIndex caller side
		//	rf.getLastLogIdx() >= args.LastIncludedIndex && args.LastIncludedIndex < rf.lastIncludedIdx
		DebugP(dSnap, "S%d, (args.LastIncludedIdx: %d), host.commitIndex %d, info: %s", rf.me, args.LastIncludedIndex, rf.commitIndex, rf.printLogInformation())
		
		if rf.commitIndex > args.LastIncludedIndex {
			reply.PeerLastIncludedIdx = rf.lastIncludedIdx
			reply.PeerLastIncludedTerm = rf.lastIncludedTerm
		}
		rf.mu.Unlock()
	}
}

func (rf * Raft) printLogInformation() string {
	logMsg := serializeLogContents(rf.logs)

	retString := fmt.Sprintf("lastInclIdx: %d, lastInclTerm: %d, logs: [%s]", rf.lastIncludedIdx, rf.lastIncludedTerm, logMsg)
	return retString
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
		adjustedIdx := rf.getInternalLogIdx(startLogIdx)
		rf.logs = append(rf.logs[:adjustedIdx])
		rf.persist()
	}
}


func (rf *Raft) getInternalLogIdx(logIdx int) int {
	adjustedIdx := logIdx - rf.lastIncludedIdx
	
	if adjustedIdx >= 0 && adjustedIdx < len(rf.logs) {
		return adjustedIdx
	}
	panicMsg := fmt.Sprintf("S%d Index %d is out of range, lastInc %d, adj: %d, logs:[%s]", rf.me, logIdx, rf.lastIncludedIdx, adjustedIdx, rf.printLogInformation())
	panic(panicMsg)
}

// We will use this with snapshot (since we'd need to truncate the logs)
//	-> (The physical log will be missing)
func (rf *Raft) getLogAtIndex(logIdx int) Log {

	if logIdx == rf.lastIncludedIdx {
		return Log{true, "", rf.lastIncludedTerm}
	}
	
	adjustedIdx := logIdx - rf.lastIncludedIdx

	if adjustedIdx >= 0 && adjustedIdx < len(rf.logs) {
		return rf.logs[adjustedIdx]
	} 
	
	panicMsg := fmt.Sprintf("S%d Index %d is out of range, lastInc %d, adj: %d, logs:[%s]", rf.me, logIdx, rf.lastIncludedIdx, adjustedIdx, rf.printLogInformation())
	panic(panicMsg)
}

func (rf *Raft) getLogTermAtIndex(logIdx int) int {
	if logIdx < rf.lastIncludedIdx {
		return LOG_TRUNCATED;
	}

	if logIdx == rf.lastIncludedIdx {
		return rf.lastIncludedTerm
	}

	adjustedIdx := logIdx - rf.lastIncludedIdx
	if adjustedIdx >= 0 && adjustedIdx < len(rf.logs) {
		return rf.logs[adjustedIdx].CommandTerm
	}
	panicMsg := fmt.Sprintf("S%d Index %d is out of range, logs: [%s]", rf.me, adjustedIdx, rf.printLogInformation())
	panic(panicMsg)
}

// At any given moment, there will be at least one entry in rf.logs
//	because raft logs are index'd by 1
func (rf *Raft) getLastLogIdx() int {
	return rf.lastIncludedIdx + len(rf.logs) -1
}

func (rf *Raft) getLastLog() Log {
	lastLogIdx := rf.getLastLogIdx()
	return rf.getLogAtIndex(lastLogIdx)
}

func (rf *Raft) getLastLogTerm() (int) {
	log := rf.getLastLog()
	return log.CommandTerm
}

// Retrieve all logs starting at some log index

// leader get logs starting at rf.lastIncludedIndex
func (rf *Raft) getLeaderLogs(startLogIdx int) []Log {
	if startLogIdx >= rf.getLastLogIdx() + 1 {
		DPrintf(LOG_LEVEL_WARN, "Warning! attempting to retrieve log out of bound at idx: %d", startLogIdx)
	}

	adjustedIdx := rf.getInternalLogIdx(startLogIdx)
	logsToReturn := rf.logs[adjustedIdx:]

	newLogs := make([]Log, len(logsToReturn))
	
	for i :=0; i <len(logsToReturn); i++ {
		newLogs[i].CommandTerm = logsToReturn[i].CommandTerm
		newLogs[i].Command = logsToReturn[i].Command 
		newLogs[i].CommandValid = logsToReturn[i].CommandValid
	}

	return newLogs
}

// trims the log (if any)
//  -> Will also reset lastIncludedIdx/term
func (rf *Raft) trimLogAt(prefix int) {
	if prefix <= rf.lastIncludedIdx	|| prefix > rf.getLastLogIdx() {
		return
	}
	
	lastInclTerm := rf.getLogTermAtIndex(prefix)
	internalLogPtr := prefix - rf.lastIncludedIdx
	
	newLogSlice := []Log{Log{true, rf.logs[internalLogPtr].Command, lastInclTerm}}
	shrunkenSlice := rf.logs[internalLogPtr + 1:]
 
	mergedSlice := make([]Log, len(newLogSlice)+len(shrunkenSlice))
	copy(mergedSlice, newLogSlice)
	copy(mergedSlice[len(newLogSlice):], shrunkenSlice)
	rf.logs = mergedSlice

	rf.lastIncludedIdx = prefix
	rf.lastIncludedTerm = lastInclTerm
}

// Converge logs from leader
// TODO: Think through how this will work with new system
//			-> Correction: We actually need a stub to have this work properly
/**
	rf.lastIncludedIdx: 5
	rf.lastIncludedTerm: 3,
	rf.logs =[{stub 0}]

		-> technically have 5 logs (DONT INCLUDE THE STUB)
**/
func (rf *Raft) acceptLogsFromLeader(leaderLogs *[]Log, startLogIdx int) int {
	logsFromLeader := *leaderLogs
	startIdx := 0

	if startLogIdx <= rf.lastIncludedIdx {
		return -1
	}
	 
	for hostLogIdxStart := startLogIdx; hostLogIdxStart <= rf.getLastLogIdx(); hostLogIdxStart++ {
		// Logs with conflicting term found!
		if startIdx < len(logsFromLeader) &&  rf.getLogTermAtIndex(hostLogIdxStart) != logsFromLeader[startIdx].CommandTerm {
			adjustedIdx := rf.getInternalLogIdx(hostLogIdxStart)
		 
			rf.logs = rf.logs[:adjustedIdx]
			rf.logs = append(rf.logs, logsFromLeader[startIdx])
		}
		startIdx++
	}

	for ;startIdx < len(logsFromLeader); startIdx++ {
		rf.logs = append(rf.logs, logsFromLeader[startIdx])
	}
	rf.persist()
	return startLogIdx + len(logsFromLeader)
}

func (rf * Raft) printLogContent() {
	for i := 0; i < len(rf.logs); i++ {
		DPrintf(LOG_LEVEL_WARN, "Host: %d Content: %s, Term: %d ", rf.me, rf.logs[i].Command, rf.logs[i].CommandTerm)
	}
}

// ********************************************************//


// A raft host will only Snapshot up-to-and-include some index, iff
//	-> the index is greater than the host.lastIncludedIdx
//		-> Don't fallback to an older snapshot
//	-> equal to the host.commitINdex
//		-> Only snapshot if index has already been applied
//	-> index is within the range
// Reject otherwise

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
 
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIdx || index != rf.lastApplied || rf.getLastLogIdx() < index {
		return
	}

	DebugP(dCommit, "S%d received snapshot request %d, lastApplied %d, lastInclIdx: %d, logs:[%s]", rf.me, index, rf.lastApplied, rf.lastIncludedIdx, rf.printLogInformation())
 
	rf.trimLogAt(index)
	DebugP(dCommit, "S%d post trim data: LastApplied: %d, lastInlcudedIdx: %d , logs: [%s]", rf.me, rf.lastApplied, rf.lastIncludedIdx, rf.printLogInformation())
	// This state persistence logic is probably incorrect. TODO fix
	raftstate := rf.getRaftState()
	rf.persister.SaveStateAndSnapshot(raftstate, snapshot)
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

type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	Data []byte
}

type InstallSnapshotReply struct {
	Term int
	Success bool
	PeerLastIncludedIdx int
	PeerLastIncludedTerm int
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
 
	if candidate_term > rf.currentTerm {
		DebugP(dElection,"S%d (%s) term is lower than candidate, reset to follower", rf.me, GetServerState(rf.nodeStatus))
		rf.setStateToFollower(candidate_term)
	}

	reply.Term = host_term

	if candidate_term < host_term {
		reply.VoteGranted = false 
	} else {
 
		if rf.nodeStatus == Follower && (rf.votedFor == HAS_NOT_VOTED || rf.votedFor == args.CandidateId) && rf.candidateLogIsUpToDate(args.LastLogIndex, args.LastLogTerm, candidate_term) {
			 
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.lastContactWithPeer = time.Now()
			rf.persist()
			DebugP(dElection,"S%d granted vote for S%d, term %d", rf.me, rf.votedFor, rf.currentTerm)
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
 
	reply.Term  = rf.currentTerm
	reply.LogConsistent = true
	reply.Xterm = -1
	reply.XIndex = -1
	reply.Xlen = -1
	DebugP(dReplica,"S%d received AE from S%d", rf.me, args.LeaderId)
	if candidate_term < host_term {
		DebugP(dElection,"Reject AE from Leader %d due to term mismatch", args.LeaderId)
		reply.Success = false
		rf.mu.Unlock()
		return 
	}

	if candidate_term > host_term {
		rf.setStateToFollower(candidate_term)
		rf.leaderId = args.LeaderId
	}

	if rf.serverContainsLeaderLog(args.PrevLogIndex, args.PrevLogTerm) == false {
		DebugP(dReplica,"S%d conflicts with index on index %d, term %d",rf.me, args.PrevLogIndex,args.PrevLogTerm)
		DebugP(dReplica,"S%d state: [%s]", rf.me, rf.printLogInformation())

		// TODO: With snap shots we may need to detect when InstallingSnapShot is needed
		rf.lastContactWithPeer = time.Now()
		rf.leaderId = args.LeaderId
		reply.Success = false
		reply.LogConsistent = false
 
		if rf.getLastLogIdx() < args.PrevLogIndex {
			// This is the case where the follower's log is too short.
			//   -> However, we should probably set the reply.Xlen to rf.getLastLogIdx() + 1
			//
			reply.Xlen = rf.getLastLogIdx() + 1
		} else {
			conflict_term := rf.getLogTermAtIndex(args.PrevLogIndex)
			first_idx_with_conflict := rf.lookupFirstEntryWithTerm(conflict_term)

			reply.Xterm = conflict_term
			reply.XIndex = first_idx_with_conflict
		}

	} else if rf.serverContainsLeaderLog(args.PrevLogIndex, args.PrevLogTerm) == true {
 
		rf.lastContactWithPeer = time.Now()
		rf.leaderId = args.LeaderId
		
		reply.LogConsistent = true
		reply.Success = true

		rf.acceptLogsFromLeader(&args.Entries, args.PrevLogIndex + 1)
 
		if args.LeaderCommit > rf.commitIndex {
			lastNewLogIdx := args.PrevLogIndex + len(args.Entries)
			rf.commitIndex = min(args.LeaderCommit, lastNewLogIdx)
			DebugP(dSnap,"S%d agrees with S%d, idx %d, term %d", rf.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm)
			DebugP(dSnap, "S%d, %s", rf.me, rf.printLogInformation())
			DebugP(dSnap, "S%d set commit index to %d", rf.me,  rf.commitIndex)
			DebugP(dSnap, "S%d recv logs [%s] from leader %d, T: %d, prevLogIdx: %d, prevLogTerm: %d", rf.me, serializeLogContents(args.Entries), args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm)
			//DebugP(dCommit, "S%d, lastInclIdx: %d, lastInclTerm: %d, log content: [%s]", rf.me, rf.lastIncludedIdx, rf.lastIncludedTerm, serializeLogContents(rf.logs))
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
	
	// Check
	if leaderPrevLogIdx < 0 || leaderPrevLogTerm < 0 {
		return false
	}

	if rf.getLastLogIdx() < leaderPrevLogIdx {
		DebugP(dReplica,"S%d logs (len: %d): [%s] are shorter than leader logs %d", rf.me, len(rf.logs), serializeLogContents(rf.logs), leaderPrevLogIdx)
		return false
	}
	
	if rf.getLogTermAtIndex(leaderPrevLogIdx) != leaderPrevLogTerm {
		DebugP(dReplica,"S%d logs: [%s] has a conflict at index %d, (HostTerm: %d, LeaderTerm:%d)",
		rf.me, 
		serializeLogContents(rf.logs),
		leaderPrevLogIdx,
		rf.getLogTermAtIndex(leaderPrevLogIdx),
		leaderPrevLogTerm)
		return false	
	}

	return true
}


// TODO:
//	-> Rewrite this using binary search
func (rf *Raft) lookupFirstEntryWithTerm(xTerm int) int {

	if rf.lastIncludedTerm == xTerm {
		return rf.lastIncludedIdx
	}

	for i:= 0; i< len(rf.logs); i++ {
		if rf.logs[i].CommandTerm == xTerm {
			return i + rf.lastIncludedIdx
		}
	}

	return -1
}

// Reserved for leader
func (rf * Raft) lookupLastEntryWithTerm(xTerm int) int {

	if xTerm == rf.lastIncludedTerm {
		return rf.lastIncludedIdx
	}

	for i := len(rf.logs) - 1; i >=0; i-- {
		if rf.logs[i].CommandTerm == xTerm {
			return i + rf.lastIncludedIdx
		}
	}
 
	return -1
}

// What if we design an exponeial backoff wrt number of failed tries?
func (rf *Raft) getNewElectionTimeout() int {
	 
	
	//return (int)(150 + rand.Int31n(150))
	 return RandRange(150,300)
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
	defer rf.mu.Unlock()

	if rf.currentTerm != args.Term {
		DebugP(dDrop, "S%d Term mistmatch found! Drop RPC reply", rf.me)
 
		return false
	}

	if reply.Term > rf.currentTerm {
		DebugP(dElection,"S%d term(%d)is higher than candidate S%d, term (%d) stepping down", server, reply.Term, rf.me, rf.currentTerm)
		rf.setStateToFollower(reply.Term) 
		return false;
	}

	if rf.nodeStatus == Candidate && reply.VoteGranted == true {
		DebugP(dElection,"S%d received votes from S%d for term %d", rf.me, server, args.Term)
		rf.votesReceived++
	}
 	 
	rf.lastContactWithPeer = time.Now()
 	
	return ok
}


/**

	prevLogIndex = rf.lastIncludedIdx
	prevLogTerm = rf.lastIncludedTerm
**/
func (rf *Raft) scanNextIndex() {
	leaderLastLogIdx := rf.getLastLogIdx()
	for peerId := range rf.peers {
		if peerId != rf.me && leaderLastLogIdx >= rf.nextIndex[peerId] {
			prevLogIdx := rf.nextIndex[peerId] - 1
			prevLogTerm := rf.getLogTermAtIndex(prevLogIdx)
			
			if prevLogTerm == LOG_TRUNCATED {
				// DebugP(dSnap, "(SI) L%d to bring S%d up to date with lastInclIdx %d", rf.me, peerId, rf.lastIncludedIdx)
				// DebugP(dSnap, "(SI) L%d was truncated at %d, L:%s", rf.me, prevLogIdx, rf.printLogInformation())
				// installSnapshotArg := InstallSnapshotArgs{rf.currentTerm, rf.me, rf.lastIncludedIdx, rf.lastIncludedTerm, rf.persister.ReadSnapshot()}
				// installSnapshotReply := InstallSnapshotReply{}
				// go rf.sendInstallSnapshot(peerId,&installSnapshotArg, &installSnapshotReply)
			} else {
				entries := rf.getLeaderLogs(rf.nextIndex[peerId])
				DebugP(dReplica, "Leader %d sending log:[%s] to peer %d", rf.me, serializeLogContents(entries),peerId)
				initalAppendEntryRPC := AppendEntriesArgs{rf.currentTerm, rf.me, prevLogIdx, prevLogTerm, entries, rf.commitIndex}
				go rf.sendAppendEntries(peerId, &initalAppendEntryRPC, &AppendEntriesReply{})
			}
		}
	}
}


func (rf * Raft) lookForMatchIndex() {
	for rf.killed() == false {
		rf.mu.Lock()
			if rf.nodeStatus == Leader {
				rf.scanNextIndex()
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
		return ok
	}
	rf.mu.Lock()
	DebugP(dCommit,"S%d send logs %s to S%d, prevLogIdx: %d", rf.me, serializeLogContents(args.Entries), server, args.PrevLogIndex)
	DebugP(dCommit,"S%d log Info %s", rf.me, rf.printLogInformation())
	// Handles cases where we get old RPC reply
	if rf.currentTerm != args.Term {
		DebugP(dDrop,"Dropping replies from %d because conflict (CT: %d, ST: %d)", server, rf.currentTerm, args.Term)
		rf.mu.Unlock()
		return false
	}

	host_term := args.Term
	if reply.Term > host_term {
		DebugP(dElection, "S%d steps down for term %d", host_term)
		rf.setStateToFollower(reply.Term)
		rf.mu.Unlock()
		return true;
	}

	if rf.nodeStatus != Leader {
		
		rf.mu.Unlock()
		return false
	}

	if reply.Success == true {
 
		rf.lastContactWithPeer = time.Now()
 	   
		newMatchIndex := args.PrevLogIndex + len(args.Entries) 

		rf.matchIndex[server] = newMatchIndex
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		
	   DebugP(dReplica,"Setting S%d.matchIndex[S%d]=%d", rf.me, server,rf.matchIndex[server])
	   for N := rf.commitIndex + 1; N <= rf.getLastLogIdx(); N++ {
		 
			serverReplicatedCount := 0
			for peerId := range rf.peers {
				if peerId != rf.me && rf.matchIndex[peerId] >= N {
					serverReplicatedCount++
				}

				if serverReplicatedCount >= (len(rf.peers)/2) {
			
					if rf.getLogTermAtIndex(N) == args.Term {
						rf.commitIndex = N
						DebugP(dCommit, "(%d) servers has logs matched up to %d, set S%d commit index to %d",
						serverReplicatedCount,
						N,
						rf.me,
						rf.commitIndex)
						DebugP(dCommit, "Leader S%d logs: [%s]", rf.me, serializeLogContents(rf.logs))
					}
				}
			}
	   }
	   rf.mu.Unlock()
	   return true	
	   
	} else if reply.LogConsistent == false && rf.nodeStatus == Leader {

		if reply.Xterm != -1 {
			// handles the first two cases
			lookupIdx := rf.lookupLastEntryWithTerm(reply.Xterm)
			
			if lookupIdx == -1 {
				// Leader does not have xTerm
				rf.nextIndex[server] = reply.XIndex
				DebugP(dReplica,"Leader %d does not have conflict term %d with S%d, setting nextIndex[S%d] to %d",rf.me, reply.Xterm, server, server, rf.nextIndex[server])
				DebugP(dReplica,"Leader %d logs: %s", rf.me, serializeLogContents(rf.logs))
			} else {
				// Leader have xTerm
				rf.nextIndex[server] = lookupIdx + 1
				DebugP(dReplica,"Leader %d have conflict term %d, ([])", rf.me, reply.Xterm)
				DebugP(dReplica,"Leader %d logs: %s",rf.me, serializeLogContents(rf.logs))
			}

		} else if reply.Xlen != -1 {
			// handles the case where follower's log is too short
			//DebugPrintf(LOG_LEVEL_PERSISTENCE, "Hi")
			rf.nextIndex[server] = reply.Xlen
			DebugP(dReplica, "S%d log is too short. Setting rf.nextIndex[S%d]=%d", server,server, rf.nextIndex[server])
		} else {
			failMsg := fmt.Sprintf("Error! reply.Xterm: %d , reply.Index: %d, reply.Xlen: %d", reply.Xterm, reply.XIndex, reply.Xlen)
			panic(failMsg)
		}
		
 		args.Term = rf.currentTerm
		args.PrevLogIndex = rf.nextIndex[server] - 1
		args.PrevLogTerm = rf.getLogTermAtIndex(args.PrevLogIndex)
		
		if args.PrevLogTerm == LOG_TRUNCATED {
			DebugP(dSnap, "(SA) L%d bring up S%d up to date with lastInclIdx %d", rf.me, server, rf.lastIncludedIdx)
			installSnapshotArg := InstallSnapshotArgs{rf.currentTerm, rf.me, rf.lastIncludedIdx, rf.lastIncludedTerm, rf.persister.ReadSnapshot()}
			installSnapshotReply := InstallSnapshotReply{}
			go rf.sendInstallSnapshot(server, &installSnapshotArg, &installSnapshotReply)
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		} else {
			args.Entries = rf.getLeaderLogs(rf.nextIndex[server])
			rf.mu.Unlock()
		}

		// rf.mu.Unlock()		
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

	DebugP(dReplica, "Leader %d append command %s for term %d", rf.me, command, term)

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

func (rf *Raft) dedicatedApplier(applyMsgs [] ApplyMsg) {
	for _, v := range applyMsgs {
		rf.applyCh <- v
		rf.mu.Lock()
		rf.lastApplied = v.CommandIndex
		DebugP(dLast, "S%d commiting log, updating lastApplied to %d", rf.me, rf.lastApplied)
		
		DebugP(dCommit, "S%d applying log [(%s)] at term %d, CI %d, isLeader: %v", rf.me, v.Command , rf.currentTerm, rf.lastApplied, rf.nodeStatus == Leader)
		rf.mu.Unlock()
	}
}

 
// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) lifeCycleManager() {
	for rf.killed() == false {

		rf.mu.Lock()
		 	
		if rf.commitIndex > rf.lastApplied {
			applyMsgs := make([]ApplyMsg,0)
			
			lastApplied := rf.lastApplied
			DebugP(dLast,"S%d lastApplied is %d", rf.me, rf.lastApplied)
			for lastApplied < rf.commitIndex {
				lastApplied++
				logEntryToCommit := rf.getLogAtIndex(lastApplied)
				applyMsg := ApplyMsg{true, logEntryToCommit.Command, lastApplied, false,nil, 0,0}	
 
				applyMsgs = append(applyMsgs, applyMsg)
			}
			go rf.dedicatedApplier(applyMsgs) 
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
					DebugP(dElection,"Election timeout: %d without electing leader for term %d", rf.electionTimeout, rf.currentTerm)
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

		time.Sleep(50 * time.Millisecond)
	}
}

// Shrink the size of the log
/**
	NOTE:
		-> We should not add a stub any more
		-> Consider the following:
			-> rf.logs = [0 1 2 3 4 5]
			-> follower recv snapshot RPC from leader and has to discard first 2 entries
			-> removeLogPrefix(2)
				-> really we need to remove logs [0 1 2]
				-> [3,4,5], lastIncludedIndex = 2, lastIncludedTerm = 2
				-> lastLogIdx()
					-> (current):
						-> len(rf.logs) - 1 
							-> 6 - 1
							-> 5 

					-> (with snapshot)
						-> lastIncludedIndex = 2
						-> len(rf.logs) = 3
						-> lastIncludedIndex + len(rf.logs) = 5
				-> getLastLogTerm()
					-> rf.getLastLogIdx() = 5
					-> (current):
						-> rf.logs[5].CommandTerm
							-> 5
					-> (with snapshot)
						-> lastIncludedIndex = 2
						-> internalLogPtr := 5 - lastIncludedIndex 
										  := 5 -2 
										  := 3
**/

func (rf *Raft) removeLogPrefix(lastIncludedIdx int) {
	if lastIncludedIdx >= len(rf.logs) {
		rf.logs = make([]Log, 0)
		rf.AppendNewLog(Log{true,"Stub",0})
		return 
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
	DebugP(dElection,"L%d broadcast HB for term %d", rf.me, rf.currentTerm)

	for peerId := range rf.peers {
		if peerId != leaderId {
			prevLogIdx := nextIndexArr[peerId] - 1
			prevLogTerm := rf.getLogTermAtIndex(prevLogIdx)
			
			if prevLogTerm == LOG_TRUNCATED {
				DebugP(dSnap, "(HB) L%d to bring S%d up to date with lastInclIdx %d", rf.me, peerId, rf.lastIncludedIdx)
				DebugP(dSnap, "(HB) S%d prevLogIdx %d ", peerId, prevLogIdx)
				DebugP(dSnap, "(HB) L%d: %s", rf.me, rf.printLogInformation())
				installSnapshotArg := InstallSnapshotArgs{rf.currentTerm, rf.me, rf.lastIncludedIdx, rf.lastIncludedTerm, rf.persister.ReadSnapshot()}
				installSnapshotReply := InstallSnapshotReply{}
				go rf.sendInstallSnapshot(peerId,&installSnapshotArg, &installSnapshotReply)
			} else {
				appendEntriesArgs := AppendEntriesArgs{leaderTerm, leaderId, prevLogIdx, prevLogTerm, emptyEntries, rf.commitIndex}
				go rf.sendAppendEntries(peerId, &appendEntriesArgs, &AppendEntriesReply{})
			}
		}
	}
}

func (rf * Raft) candidateTransitionToLeader() {
	rf.leaderId = rf.me 
	rf.nodeStatus = Leader
	 
	// leaderLastLogIdx := rf.getLastLogIdx() + 1
	DebugP(dElection, "S%d became leader for term %d", rf.me, rf.currentTerm)

	for peerId := range rf.peers {
		if peerId != rf.me {
			rf.nextIndex[peerId] = rf.getLastLogIdx() + 1
			rf.matchIndex[peerId] = 0
		}
	}
	 
	rf.leaderSendHeartBeatMessages(rf.me, rf.currentTerm, &rf.nextIndex,&rf.logs)
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

	rf.lastIncludedIdx = 0
	rf.lastIncludedTerm = 0
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
