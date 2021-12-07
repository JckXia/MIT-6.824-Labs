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
	"math"
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

type Log struct {
	Command interface {}
	TermNumber int
}

func getCurrentTimeStamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
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

	logs []Log
	
	commitIndex int
	lastApplied int
	nextIndex [] int
	matchIndex []int 

	applyMsgChan chan ApplyMsg
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

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
	PrevLogIndex int
	PrevLogTerm int
	Entries [] Log
	LeaderCommit int
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
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term int32
	VoteGranted bool
}

// AppendEntries handler
// So what we are getting here is that...
// We should ONLY reset the election timer if reply.Success= true
// If it does not pass the checks (term check, w.e) we should not reset the election timer
func (rf * Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	 
 
	if rf.currentTerm > args.Term {
		 
		reply.Success = false
	} else {
 
		// currentTerm <= args.Term. Need to convert to Follower 
		// Contacted by the current leader it seems

		rf.votesCnt = 0
		rf.lastContactFromLeader = getCurrentTimeStamp()
		rf.electionTimeout = rf.getElectionTimeout()
		rf.currentTerm = args.Term
		rf.electionState = Follower
		// Empty entries means heart beat. Do NOT handle this like so.
		// PrevLogIndex should be 0 in the beginning. Since it's empty
		
		if len(rf.logs) >= args.PrevLogIndex {
			if len(rf.logs) == 0 || rf.logs[args.PrevLogIndex].TermNumber == args.PrevLogTerm {
				for _, entry := range args.Entries {
					rf.logs = append(rf.logs, entry)
				}

				if args.LeaderCommit > rf.commitIndex {
					leaderCommit := float64(args.LeaderCommit)
					lastEntryIdx := float64(len(rf.logs) - 1)
					rf.commitIndex = int(math.Min(leaderCommit, lastEntryIdx))
				}

				reply.Success = true
			} else {
				// Conflicting entries. Delete entries after Prev Log Index
				rf.logs = rf.logs[args.PrevLogIndex : len(rf.logs)]
				reply.Success = false
			}
		} else {
			// Do nothing
			reply.Success = false
		}
 
	}
	rf.mu.Unlock()
	
}

// Candidate's log is at least as up to date as receiver's log 
// Compare the index and term of the last entries in the logs

// If logs have last entries with diff terms. 
// 		The log with the later term is more up to date
//		If logs end with the same term, whichever log is longer is more up to date
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
	} else {
		 rf.currentTerm = args.Term
		 rf.electionState = Follower
		if ( rf.votedFor == -1 || rf.votedFor == args.CandidateId ) && !rf.ReceiverIsMoreUpToDate(args) {
			rf.lastContactFromLeader = getCurrentTimeStamp()
			rf.electionTimeout = rf.getElectionTimeout()
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		}
	}

	rf.mu.Unlock()
}

func (rf * Raft) ReceiverIsMoreUpToDate(args *RequestVoteArgs) bool {
	receiverLastIndex := len(rf.logs) - 1
	if receiverLastIndex < 0 {
		return false
	}

	receiverLastTerm := rf.logs[receiverLastIndex].TermNumber

	senderLastIndex := args.LastLogIndex
	senderLastTerm := args.LastLogTerm

	if receiverLastTerm > senderLastTerm  {
		return true
	} else if receiverLastTerm == senderLastTerm && receiverLastIndex > senderLastIndex {
		return true
	} 
	return false
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
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
 
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

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

func (rf * Raft) startElection() {
	rf.electionState = Candidate
	rf.currentTerm +=1
	rf.votedFor = int32(rf.me)
	rf.votesCnt = 1
}

func (rf * Raft) revertToFollower(newTerm int) {
	rf.electionState = Follower
	rf.currentTerm = int32(newTerm)
	rf.votedFor = -1
	rf.lastContactFromLeader = getCurrentTimeStamp()
	rf.electionTimeout = rf.getElectionTimeout()
}

func (rf  * Raft) convToCandidate() {

}	

// The ticker go routine starts a new election if this peer hasn't received
// Logic surrounding election start

// lastContactFromLeader --- currentTimeStamp  -- lastContactFromLeader + electionTimeout
 
func (rf *Raft) ticker() {
 
	for rf.killed() == false {
	 
		rf.mu.Lock()
		 
		currentTimeStamp := getCurrentTimeStamp()
		elapsedTime := int(currentTimeStamp - rf.lastContactFromLeader)
		
		// Follower converts to candidate
		// Do not increment counter if already leader
		if elapsedTime >= rf.electionTimeout && rf.electionState != Leader {

			rf.currentTerm += 1
			rf.votesCnt = 1
			rf.votedFor = int32(rf.me) 
			rf.lastContactFromLeader = getCurrentTimeStamp()
			rf.electionTimeout = rf.getElectionTimeout()
			rf.electionState = Candidate
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
		// rf.mu.Lock()
		electionState := rf.electionState
		currTerm := rf.currentTerm
		candidateId := int32(rf.me)
		serverCnt := len(rf.peers)
		elecCompl := false

		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied += 1
			// Construct a message to send to applMsgChan
		}

		if electionState == Candidate {
			
			if serverCnt == 0 {
				rf.mu.Unlock()
				return
			}
			 
			rf.mu.Unlock()	
			for serverId := 0; serverId < serverCnt; serverId++ {
				reqVoteArgs := RequestVoteArgs{currTerm, candidateId, -1,-1}
				reqVoteReply := RequestVoteReply{}
				
				rf.mu.Lock()
				if elecCompl {
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()

				if int(candidateId) != serverId {
					go func(serverId int) {
						// This potentially could dead lock
						 
						status := rf.sendRequestVote(serverId, &reqVoteArgs, &reqVoteReply)
						rf.mu.Lock()
						if status && reqVoteReply.VoteGranted {
							rf.votesCnt++
							
							if rf.votesCnt > serverCnt / 2 {
								rf.electionState = Leader
								elecCompl = true
							}
						} else if reqVoteReply.Term > currTerm {
							rf.currentTerm = reqVoteReply.Term
							rf.electionState = Follower
							rf.votedFor = -1
							rf.votesCnt = 0
							rf.lastContactFromLeader = getCurrentTimeStamp()
							rf.electionTimeout = rf.getElectionTimeout()
						}
						rf.mu.Unlock()
					}(serverId)
				}
			}
		 
			 
		} else if electionState == Leader {
			 
			 
		 
			rf.mu.Unlock()
			for serverId := 0; serverId < serverCnt; serverId++ { 
				var entr [] Log;
				appendEntrArgs := AppendEntriesArgs{currTerm, candidateId, 0,0,entr,0}
				appendEntrReply := AppendEntriesReply{}
				// rf.mu.Unlock()
				if int(candidateId) != serverId { 
					go func(serverId int) {
						// This potentially could dead lock
						// fmt.Println(appendEntrArgs,"  ", appendEntrReply)
						rf.sendAppendEntry(serverId, &appendEntrArgs, &appendEntrReply)
						// fmt.Println("Here! ") 
						// rf.mu.Lock()
						rf.mu.Lock()
						if !appendEntrReply.Success && appendEntrReply.Term > currTerm {
							 
							rf.currentTerm = appendEntrReply.Term
							rf.electionState = Follower
							rf.votedFor = -1
							rf.votesCnt = 0
							rf.lastContactFromLeader = getCurrentTimeStamp()
							rf.electionTimeout = rf.getElectionTimeout()

						}
						rf.mu.Unlock()
					}(serverId)
				}				 
			}
		 
		}  else {
			
			rf.mu.Unlock()
		}
		time.Sleep(1 * time.Millisecond)
 
		
	}
 
}


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

	rf.commitIndex = 0 
	rf.lastApplied = 0

	rf.applyMsgChan = applyCh
	// Your initialization code here (2A, 2B, 2C).
 
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
    go rf.RPCReqPoll()


	return rf
}
