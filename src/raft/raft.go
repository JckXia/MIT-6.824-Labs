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
     "fmt"
	"sync"
//	"math"
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
	mu        sync.RWMutex          // Lock to protect shared access to this peer's state
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

	peerWonElectionSig chan bool
	peerGrantVoteCh chan bool
	peerHeartBeatCh chan bool
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

// Args and reply checks out
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

// could've been a lot more complex
func (rf * Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		
	} else {
		reply.Term = rf.currentTerm
		reply.Success = true
	}
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
		} else {
			// We should ONLY reset the election timeout if and only if we are granting a vote
			// to a candidate. Else, let it elapse
			reply.VoteGranted = false
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

// This function should handle logic with respect with responses, etc
func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
 
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	
	if !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
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


func (rf *Raft) Start(command interface{}) (int, int, bool) {
 
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := bool(rf.electionState == Leader)

	if !isLeader {
		return -1, int(rf.currentTerm), false
	}
	
	term := int(rf.currentTerm)

	newLog := Log{command, term}
	rf.logs = append(rf.logs, newLog)
	
	index := len(rf.logs) - 1
//	fmt.Println("Leader ", rf.me," append log res ", rf.logs," index ", index)
	return index, term, isLeader
}

// If lats log index >= nextIndex for a follower...
// 		Send AppendEntries RPC with log entries starting at nextIndex
//		If successful, update nextIndex and matchIndex for followers
//      If appendEntries fails because of log inconsistency, decrement nextIndex and retry

// If there exists and N such that N> commitIndex, 
// A majority of matchIndex[i] >= N, and log[N].term == currentTerm,
// set commitIndex = N

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


// The ticker go routine starts a new election if this peer hasn't received
// Logic surrounding election start

// lastContactFromLeader --- currentTimeStamp  -- lastContactFromLeader + electionTimeout
// This is the goroutine that handles election timeouts
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

// This is okay. Because the paper want the logs to start at 1
// To decrease overhead caused by this, we start matchIndex at -1 instead of 0
func (rf * Raft) resetNextAndMatchIndex() {
	for idx, _ := range rf.nextIndex {
		rf.nextIndex[idx] = len(rf.logs)
		rf.matchIndex[idx] = -1
	}
}
 

// This is for handling logic around servers
func (rf *Raft) RPCReqPoll() {
	for rf.killed() == false {
		



		rf.mu.Lock()

		if rf.commitIndex > rf.lastApplied {
			 
			// rf.lastApplied = 1 + rf.lastApplied
			// fmt.Println("Server ", rf.me," applies message ", rf.logs[rf.lastApplied].Command)
			rf.lastApplied++
			lastAppliedVal := rf.lastApplied 
			commandToApply := rf.logs[lastAppliedVal].Command

			go func() {
			 
				applyMsg := ApplyMsg{true, commandToApply, lastAppliedVal, false, nil,0,0}
				rf.applyMsgChan <- applyMsg
				 
			}()
		}

		electionState := rf.electionState
		currTerm := rf.currentTerm
		candidateId := int32(rf.me)
		serverCnt := len(rf.peers)
		elecCompl := false

 

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
							
							// Election successful?
							// Need to reset nextIndex and matchIndex
							if rf.votesCnt > serverCnt / 2 {
								rf.electionState = Leader
								rf.resetNextAndMatchIndex() 
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
			 
			 
			// logNextIdx :=  rf.nextIndex[serverId] 
			// prevLogIdx = logNextIdx - 1
			// prevLogTerm = rf.logs[prevLogIdx].TermNumber

			// There's a chance that electionState changed after we Unlock
			// rfNextIndexs := rf.nextIndex
			// rfLogs := rf.logs
			// rfCommitIndx := rf.commitIndex
			// This doesn't really make sense. Because once we unlock mu it's possible that electionState became
			// mutated.
			rf.mu.Unlock()

			for serverId := 0; serverId < serverCnt; serverId++ { 

				var entr [] Log;
				 
				rf.mu.Lock()
				leaderPrevLogIndx := rf.nextIndex[serverId] - 1
				leaderPrevLogTerm := -1
				if leaderPrevLogIndx >= 0 {
					leaderPrevLogTerm = rf.logs[leaderPrevLogIndx].TermNumber
				}
				
				leaderCommit := rf.commitIndex
				rf.mu.Unlock()
				appendEntrArgs := AppendEntriesArgs{currTerm, candidateId, leaderPrevLogIndx,leaderPrevLogTerm,entr,leaderCommit}
				appendEntrReply := AppendEntriesReply{}
				
				if int(candidateId) != serverId { 
					go func(serverId int) {
						
						rf.sendAppendEntry(serverId, &appendEntrArgs, &appendEntrReply)
	 
						rf.mu.Lock()
					 

						if !appendEntrReply.Success && appendEntrReply.Term > currTerm && rf.electionState == Leader {
							 
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


			matchIndexMap := make(map[int] int)

			for serverId := 0; serverId < serverCnt; serverId++ {

				 rf.mu.Lock()
				  serverNextIndex := rf.nextIndex[serverId]
 
				leaderId := int32(rf.me)
				leaderLastLogIndex := len(rf.logs) - 1
				serverMatchIndex := rf.matchIndex[serverId]

				if serverMatchIndex > rf.commitIndex  && serverMatchIndex != -1 {
					matchIndexMap[serverMatchIndex]++
				}
				rf.mu.Unlock()
				
				if leaderLastLogIndex >= serverNextIndex && leaderId != int32(serverId) {
					go func(serverId int) {
						for {		
						 
							rf.mu.Lock()
							 
							 
							
							currTerm := rf.currentTerm
							logStartIdx := 0

							if serverNextIndex >= 0 {
								logStartIdx = serverNextIndex
								 
							}  
							 

							entriesToSend := rf.logs[logStartIdx : len(rf.logs)]
								
							prevLogIdx := serverNextIndex - 1
							prevLogTerm := -1
							if prevLogIdx >= 0 {
								prevLogTerm = rf.logs[prevLogIdx].TermNumber
							}
							leaderCommitIdx := rf.commitIndex
							
							rf.mu.Unlock()
							
							appendEntrArgs := AppendEntriesArgs{currTerm, leaderId, prevLogIdx, prevLogTerm, entriesToSend, leaderCommitIdx}
							appendEntrReply := AppendEntriesReply{}
							resp := rf.sendAppendEntry(serverId, &appendEntrArgs, &appendEntrReply)
							if resp && appendEntrReply.Success {
								
							 
								rf.mu.Lock()
								//fmt.Println("Append Entr successful")
								rf.matchIndex[serverId] = prevLogIdx + len(entriesToSend)
								rf.nextIndex[serverId] = len(rf.logs)
								rf.mu.Unlock()
								
								break
							} else {

								rf.mu.Lock()
								 
								rf.nextIndex[serverId]--
								serverNextIndex = rf.nextIndex[serverId]

								rf.mu.Unlock()
							}
						}
					}(serverId)
				}
			}
			rf.mu.Lock()
			N := rf.commitIndex
			for idx, peerVal := range matchIndexMap {
				if idx > rf.commitIndex && peerVal > len(rf.peers) / 2 && rf.logs[idx].TermNumber == int(rf.currentTerm) {
					N = idx
				} 
			}

			rf.commitIndex = N
			rf.mu.Unlock()
					
		}  else {
			
			rf.mu.Unlock()
		}
		time.Sleep(1 * time.Millisecond)
 
		
	}
 
}


func (rf * Raft) broadCastAppendEntry() {
	if rf.electionState != Leader {
		return;
	}

	for peerId := 0; peerId < len(rf.peers); peerId ++ {
		if peerId != rf.me {
			
			currentTerm := rf.currentTerm
			candidateId := int32(rf.me)
			leaderPrevLogIndx := -1
			leaderPrevLogTerm := -1
			var emptyEntry [] Log;
			leaderCommitIdx := 0
			appendEntrArgs := AppendEntriesArgs {currentTerm, candidateId, leaderPrevLogIndx, leaderPrevLogTerm, emptyEntry, leaderCommitIdx}
			appendEntrReply := AppendEntriesReply{}
			go rf.sendAppendEntry(rf.me, &appendEntrArgs, &appendEntrReply)
		}
	}
}

func (rf *Raft) fetchElectionTimeout() time.Duration {
	min := 400
	max := 500
	rand.Seed(makeSeed())
	return time.Duration(rand.Intn(max - min + 1) + min) 
}


func (rf *Raft) getElectionTimeout() int {
	min := 400
	max := 500
	rand.Seed(makeSeed())
	return rand.Intn(max - min + 1) + min
}

func (rf * Raft) stepDownToFollower() {
	state := rf.state
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1

	
}

func (rf * Raft) startPeerService() {
	for rf.killed() == false {
		rf.mu.Lock()
		peerState := rf.electionState
		rf.mu.Unlock()
		switch peerState {
			case Leader:
				select {
					case <-time.After(120 * time.Millisecond):
					   // rf.broadCastAppendEntry()
					}
			case Candidate:
				select { 
					case <-rf.peerWonElectionSig:
						// rf.convertToLeader()
					case <-time.After(rf.fetchElectionTimeout() * time.Millisecond):
						// rf.converToCandidate()		
				}
			case Follower:
				select {
					case <-rf.peerHeartBeatCh:
					case <-rf.peerHeartBeatCh:
					case <-time.After(rf.fetchElectionTimeout() * time.Millisecond):
						// rf. converToCandidate()
				}
		}
	}
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

	rf.commitIndex = -1 
	rf.lastApplied = -1

	rf.applyMsgChan = applyCh

	rf.nextIndex = make([] int, len(rf.peers))
	rf.matchIndex = make([] int, len(rf.peers))
	
	// Set up channels
	rf.peerWonElectionSig = make(chan bool)
	rf.peerGrantVoteCh = make(chan bool)
	rf.peerHeartBeatCh = make(chan bool)
	// Your initialization code here (2A, 2B, 2C).
 
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
    go rf.RPCReqPoll()
	fmt.Printf("")

	return rf
}
