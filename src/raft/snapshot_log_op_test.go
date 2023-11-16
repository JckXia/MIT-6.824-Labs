package raft 

import "testing"
// import "fmt"
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



