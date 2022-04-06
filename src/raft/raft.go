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
//	"fmt"
	"sync"
	"time"
	"sync/atomic"
	"math/rand"
//	"6.824/labgob"
	"6.824/labrpc"
)

// Server states
const (
	Follower = iota
	Candidate
	Leader
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

func getCurrentTimeStamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

type Log struct {
	Command interface{}
	TermNumber int
}

// This struct handles data around log replication
// the first logs in this logs is "1"
// Initially there are no logs, so last log index is 0 
// nextIndex is then initialized to 1
type LogManager struct {
	logs []Log
	commitIndex  int
	lastApplied  int
	nextIndex []int
	matchIndex []int
}

func(lg *LogManager) getLastLogIndex() (int) {
	return len(lg.logs) - 1
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
	electionState int			// Follower, Candidate, Or Leader	
	currentTerm int32
	votedFor int32
	votesCnt int
	lastContactFromLeader int64 
	electionTimeout int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	logManager *LogManager

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer  rf.mu.Unlock()
 
	term := int(rf.currentTerm)
	isleader := bool(rf.electionState == Leader)
	 
	
	return term, isleader
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

type AppendEntriesArgs struct {
	Term int32
	LeaderId int32

	PrevLogIndex int32
	
	PrevLogTerm int32
	Entries []Log

	LeaderCommit int32
}

type AppendEntriesReply struct {
	Term int32
	Success bool
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int32
	CandidateId int32
	LastLogIndex int32
	LastLogTerm int32
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term int32
	VoteGranted bool
}

// AppendEntries handler This is the receiver implementation
func (rf * Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	 
 
	if rf.currentTerm > args.Term {
		 
		reply.Success = false
	} else {
		 
		rf.revertToFollower(args.Term)
		
		reply.Success = true
	}
 
	rf.mu.Unlock()
	
}

// RPC receiver implementation
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
	} else {
		 rf.currentTerm = args.Term
		 rf.electionState = Follower
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			rf.lastContactFromLeader = getCurrentTimeStamp()
			rf.electionTimeout = rf.getElectionTimeout()
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		}
	}

	rf.mu.Unlock()
}

 
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
 
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		 return ok
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if (reply.Term > args.Term) {
		rf.revertToFollower(reply.Term)
	} else if (reply.VoteGranted) {
		rf.votesCnt++
		if rf.votesCnt > len(rf.peers) / 2 {
			rf.convertToLeader()
		}
	} 

	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
 
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()  
	defer rf.mu.Unlock()

	if !reply.Success && reply.Term > args.Term {
		rf.revertToFollower(reply.Term)
	}
 
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


func (rf * Raft) revertToFollower(newTerm int32) {
	rf.electionState = Follower
	rf.currentTerm = int32(newTerm)
	rf.votedFor = -1
	rf.votesCnt = 0
	rf.lastContactFromLeader = getCurrentTimeStamp()
	rf.electionTimeout = rf.getElectionTimeout()
}

// current raft instance can be considered as a candidate
func (rf * Raft) convertToCandidate() {
	rf.votesCnt = 1
	rf.votedFor = int32(rf.me) 
	rf.lastContactFromLeader = getCurrentTimeStamp()
	rf.electionTimeout = rf.getElectionTimeout()
	rf.electionState = Candidate
}

func (rf *Raft) convertToLeader() {
	rf.electionState = Leader
	leaderLastLogIndex := rf.logManager.getLastLogIndex()
	
	for idx, _ := range rf.logManager.nextIndex {
		rf.logManager.nextIndex[idx] = leaderLastLogIndex + 1
		rf.logManager.matchIndex[idx] = 0
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// Logic surrounding election start

func (rf *Raft) ticker() {
 
	for rf.killed() == false {
	 
		rf.mu.Lock()
		 
		currentTimeStamp := getCurrentTimeStamp()
		elapsedTime := int(currentTimeStamp - rf.lastContactFromLeader)
		
		// Follower converts to candidate
		// Do not increment counter if already leader
		if elapsedTime >= rf.electionTimeout && rf.electionState != Leader {

			rf.currentTerm += 1
			rf.convertToCandidate()
			rf.mu.Unlock()
			
		} else {
			// ElectionTimeout isn't comlete yet
			elecTimOut := rf.electionTimeout
			rf.mu.Unlock()
			time.Sleep(time.Duration(elecTimOut - elapsedTime) * time.Millisecond)	
		}
		 
	}
}

// For sending RPC requests as "leader"
func (rf *Raft) RPCReqPoll() {
	for rf.killed() == false {
		
		rf.mu.Lock()
		electionState := rf.electionState
		currTerm := rf.currentTerm
		candidateId := int32(rf.me)
		serverCnt := len(rf.peers)
		
		if electionState == Candidate {
 
			rf.mu.Unlock()	
			for serverId := 0; serverId < serverCnt; serverId++ {
				reqVoteArgs := RequestVoteArgs{currTerm, candidateId,-1,-1}
				reqVoteReply := RequestVoteReply{}
				
				if int(candidateId) != serverId {
					go rf.sendRequestVote(serverId, &reqVoteArgs, &reqVoteReply)
				}
			}
		 
			 
		} else if electionState == Leader {
			 
			rf.mu.Unlock()
			for serverId := 0; serverId < serverCnt; serverId++ { 
				appendEntrArgs := AppendEntriesArgs{currTerm, candidateId, -1,-1, nil, -1}
				appendEntrReply := AppendEntriesReply{}

				if int(candidateId) != serverId { 
					go rf.sendAppendEntry(serverId, &appendEntrArgs, &appendEntrReply)
				}				 
			}
		 
		}  else {
			
			rf.mu.Unlock()
		}
		time.Sleep(1 * time.Millisecond)
		
	}
 
}

// min := 550
// max := 600
func (rf *Raft) getElectionTimeout() int {
	min := 400
	max := 500
	rand.Seed(makeSeed())
	return rand.Intn(max - min + 1) + min 
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
	rf.electionState = Follower
	rf.lastContactFromLeader = getCurrentTimeStamp() // Blank slate
	rf.electionTimeout = rf.getElectionTimeout() // Get election timeout
	rf.me = me
	rf.votesCnt = 0
	rf.votedFor = -1

	rf.logManager = &LogManager{}
	// Append a mock log
	rf.logManager.logs = append(rf.logManager.logs, Log{nil,-1})
	rf.logManager.commitIndex = 0
	rf.logManager.lastApplied = 0

	rf.logManager.nextIndex = make([]int, len(rf.peers))
	rf.logManager.matchIndex = make([]int, len(rf.peers))
	// Your initialization code here (2A, 2B, 2C).
 
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
    go rf.RPCReqPoll()


	return rf
}
