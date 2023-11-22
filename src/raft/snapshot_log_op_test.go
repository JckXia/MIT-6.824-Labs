package raft 

import "testing"
import "fmt"
// import "strconv"
import "github.com/stretchr/testify/assert"

func TestRaftSnapshotLogBaseCase(t *testing.T) {
	raftInstance := generateRaftWithXLogs(t,0)
	assert := assert.New(t)

	raftInstance.lastIncludedIdx = 4
	raftInstance.lastIncludedTerm = 2

	assert.Equal(raftInstance.getLastLogIdx(), 4)
	assert.Equal(raftInstance.getLastLogTerm(),2)
}


// The example shown in raft papper (Figure 12)
// Question: What happens if leader send a request to overwrite logs?
//	-> Server will only replace committed entries in its log with new snapshot
//		-> We never overwrite committed entries
func TestRaftSnapshotLogFetchAtIndex(t *testing.T) {
	assert := assert.New(t)
	raftLeader := generateRaftLaderWithYLogs(t,0)
	
	raftLeader.lastIncludedIdx = 5
	raftLeader.lastIncludedTerm = 3
	raftLeader.logs = append(raftLeader.logs, Log{true, "Leader Write Y -> 7",3})
	raftLeader.logs = append(raftLeader.logs, Log{true, "Leader Write X -> 5",3})
		
	assert.Equal(raftLeader.getLastLogIdx(), 7)
	assert.Equal(raftLeader.getLastLogTerm(),3)

	logTerm := raftLeader.getLogTermAtIndex(2)
	assert.Equal(logTerm, LOG_TRUNCATED)

	logTerm = raftLeader.getLogTermAtIndex(6)
	assert.Equal(logTerm,3)
}

func TestRaftSnapshotLogAcceptLogsFromLeader(t *testing.T) {
	assert := assert.New(t)
	assert.Equal(1,1)
	
	raftLeader := generateRaftLaderWithYLogs(t,0)
	raftLeader.lastIncludedIdx = 5
	raftLeader.lastIncludedTerm = 3

	raftLeader.logs = append(raftLeader.logs, Log{true, "Leader Write Y -> 7",3})
	raftLeader.logs = append(raftLeader.logs, Log{true, "Leader Write X -> 5",3})
	raftLeader.logs = append(raftLeader.logs, Log{true, "Leader Write X -> 20",4})
	leaderLastLogIdx := raftLeader.getLastLogIdx()
	leaderLastLogTerm := raftLeader.getLastLogTerm()
	assert.Equal(leaderLastLogIdx, 8)
	assert.Equal(leaderLastLogTerm, 4)

	leaderLogEntries := raftLeader.getLeaderLogs(6)
	
	raftFollower := generateRaftWithXLogs(t,0)
	raftFollower.lastIncludedIdx = 5
	raftFollower.lastIncludedTerm = 3

	raftFollower.acceptLogsFromLeader(&leaderLogEntries ,6)
	fmt.Println("ok")

	assert.Equal(raftFollower.lastIncludedIdx, 5)
	assert.Equal(raftFollower.lastIncludedTerm,3)
	assert.Equal(raftFollower.getLastLogIdx(),8)
	assert.Equal(raftFollower.getLastLogTerm(),4)
}

func TestRaftSnapshotLogOverwriteLogsFromLeader(t *testing.T) {
	assert := assert.New(t)
	assert.Equal(1,1)
	
	raftLeader := generateRaftLaderWithYLogs(t,0)
	raftLeader.lastIncludedIdx = 5
	raftLeader.lastIncludedTerm = 3

	raftLeader.logs = append(raftLeader.logs, Log{true, "Leader Write Y -> 7",3})
	raftLeader.logs = append(raftLeader.logs, Log{true, "Leader Write X -> 5",3})
	raftLeader.logs = append(raftLeader.logs, Log{true, "Leader Write X -> 20",4})
	leaderLastLogIdx := raftLeader.getLastLogIdx()
	leaderLastLogTerm := raftLeader.getLastLogTerm()
	assert.Equal(leaderLastLogIdx, 8)
	assert.Equal(leaderLastLogTerm, 4)

	leaderLogEntries := raftLeader.getLeaderLogs(6)
	
	raftFollower := generateRaftWithXLogs(t,0)
	raftFollower.lastIncludedIdx = 5
	raftFollower.lastIncludedTerm = 3

	raftFollower.logs = append(raftFollower.logs, Log{true, "Follower Write Y -> 7",3})
	raftFollower.logs = append(raftFollower.logs, Log{true, "Follower Write X -> 5",3})
	raftFollower.logs = append(raftFollower.logs, Log{true, "Follower Write X -> 20",3})

	raftFollower.acceptLogsFromLeader(&leaderLogEntries ,6)
	assert.Equal(raftFollower.lastIncludedIdx, 5)
	assert.Equal(raftFollower.lastIncludedTerm,3)

	assert.Equal(raftFollower.getLastLogIdx(),8)
	assert.Equal(raftFollower.getLastLogTerm(),4)

	assert.Equal(raftFollower.getLogTermAtIndex(7),3)
}

func TestRaftSnapshotTrimLogsNoSnapshot(t * testing.T) {
	raftLeader := generateRaftLaderWithYLogs(t,0)
	assert := assert.New(t)

	raftLeader.logs = append(raftLeader.logs, Log{true, "Leader Write Y -> 7",3})
	raftLeader.logs = append(raftLeader.logs, Log{true, "Leader Write X -> 5",3})
	raftLeader.logs = append(raftLeader.logs, Log{true, "Leader Write X -> 20",4})

	assert.Equal(raftLeader.lastIncludedIdx,0)
	assert.Equal(raftLeader.lastIncludedTerm,0)

	raftLeader.trimLogAt(1)

	assert.Equal(raftLeader.lastIncludedIdx, 1)
	assert.Equal(raftLeader.lastIncludedTerm, 3)
	assert.Equal(raftLeader.getLastLogIdx(),3)
	assert.Equal(raftLeader.getLastLogTerm(), 4)
}

func TestRaftSnapshotTrimEntireLogsNoSnapshot(t * testing.T) {
	raftLeader := generateRaftLaderWithYLogs(t,0)
	assert := assert.New(t)

	raftLeader.logs = append(raftLeader.logs, Log{true, "Leader Write Y -> 7",3})
	raftLeader.logs = append(raftLeader.logs, Log{true, "Leader Write X -> 5",3})
	raftLeader.logs = append(raftLeader.logs, Log{true, "Leader Write X -> 20",4})

	assert.Equal(raftLeader.lastIncludedIdx,0)
	assert.Equal(raftLeader.lastIncludedTerm,0)

	raftLeader.trimLogAt(3)

	assert.Equal(raftLeader.lastIncludedIdx, 3)
	assert.Equal(raftLeader.lastIncludedTerm, 4)
	assert.Equal(raftLeader.getLastLogIdx(),3)
	assert.Equal(raftLeader.getLastLogTerm(), 4)
}

func TestRaftSnapshotTrimLogExistingSnapshot(t * testing.T) {
	raftLeader := generateRaftLaderWithYLogs(t,0)
	assert := assert.New(t)

	raftLeader.logs = append(raftLeader.logs, Log{true, "Leader Write Y -> 7",3})
	raftLeader.logs = append(raftLeader.logs, Log{true, "Leader Write X -> 5",3})
	raftLeader.logs = append(raftLeader.logs, Log{true, "Leader Write X -> 20",4})

	assert.Equal(raftLeader.lastIncludedIdx,0)
	assert.Equal(raftLeader.lastIncludedTerm,0)

	raftLeader.trimLogAt(1)

	assert.Equal(raftLeader.lastIncludedIdx, 1)
	assert.Equal(raftLeader.lastIncludedTerm, 3)
	assert.Equal(raftLeader.getLastLogIdx(),3)
	assert.Equal(raftLeader.getLastLogTerm(), 4)

	raftLeader.trimLogAt(3)
	assert.Equal(raftLeader.lastIncludedIdx, 3)
	assert.Equal(raftLeader.lastIncludedTerm, 4)
	assert.Equal(raftLeader.getLastLogIdx(),3)
	assert.Equal(raftLeader.getLastLogTerm(), 4)
}