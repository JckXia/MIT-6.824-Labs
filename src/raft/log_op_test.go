package raft 

import "testing"
import "strconv"
import "github.com/stretchr/testify/assert"
 

/**
	Initially, there should be only one item in the log
**/
func TestRaftLogBaseCase(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	raftInstance := cfg.rafts[0]
	assert := assert.New(t)
 
	assert.Equal(len(raftInstance.logs), 1, "There should be 1 item in the log initially")
	assert.Equal(raftInstance.getLastLogIdx(), 0, "Last Log Index should be 0")
} 

func TestRaftLogCRUDOperations(t *testing.T) {
	raftInstance :=  generateRaftWithXLogs(t,4)
	assert := assert.New(t)
	
	assert.Equal(raftInstance.getLastLogIdx(), 4, "should be 4")
	
	// Remove last log
	lastLog := raftInstance.getLastLog()
	assert.Equal(lastLog.Command, "Write X -> 4")
	 
	raftInstance.deleteLogSuffix(4)
	lastLog = raftInstance.getLastLog()
	assert.Equal(lastLog.Command, "Write X -> 3")


	raftInstance = generateRaftWithXLogs(t,4)
	// Remove log starting at 2
	raftInstance.deleteLogSuffix(2)
	lastLog = raftInstance.getLastLog()
	assert.Equal(lastLog.Command, "Write X -> 1")
}	

func TestRaftLogFetch(t *testing.T) {
	
	//   0    1   2   3   4
	//S1 Stub L1  L2  L3  L4
	raftInstance := generateRaftWithXLogs(t,4)
	assert := assert.New(t)

	// returns [L1,L2,L3,L4]
	logs := raftInstance.getLeaderLogs(1)

	for i := 1; i <= len(logs); i ++ {
		logCommand := "Write X -> " + strconv.Itoa(i)
		assert.Equal(logs[i-1].Command, logCommand)
		// DebugPrintf("%s", logs[i-1].Command)
	}
	 
	// returns [L4]
	logs = raftInstance.getLeaderLogs(4)
	assert.Equal(len(logs), 1)
	assert.Equal(logs[0].Command, "Write X -> 4")
	 
	// returns [L2,L3,L4]
	logs = raftInstance.getLeaderLogs(2)
	assert.Equal(len(logs),3)
	assert.Equal(logs[0].Command, "Write X -> 2")
	assert.Equal(logs[1].Command, "Write X -> 3")
	assert.Equal(logs[2].Command, "Write X -> 4")
}

// Test that we are able to converge if leader log is longer than follower's
/**
			0 	 1	2  3  4 5
Leader   Stub   L1 L2 L3 L4 L5 
Follower Stub   F1 F2 F3

**/
func TestRaftOverwriteMiddleLogFromLeader(t *testing.T) {
	raftLeader := generateRaftLaderWithYLogs(t, 5)
	raftFollwer := generateRaftWithXLogs(t,3)
	assert := assert.New(t)

	// Should contain logs [L2,L3,L4,L5]
	raftLeaderLogs := raftLeader.getLeaderLogs(2)
	assert.Equal(len(raftLeaderLogs),4)
	assert.Equal(raftLeaderLogs[0].Command, "Leader Write X -> 2")
	assert.Equal(raftLeaderLogs[1].Command, "Leader Write X -> 3")
	assert.Equal(raftLeaderLogs[2].Command, "Leader Write X -> 4")
	assert.Equal(raftLeaderLogs[3].Command, "Leader Write X -> 5")

	// raft follower log should look like
	//	[S, F1,L2,F3,L4,L5]
	raftFollwer.acceptLogsFromLeader(&raftLeaderLogs, 2)
 
	assert.Equal(len(raftFollwer.logs), 6)

	 assert.Equal(raftFollwer.logs[1].Command, "Write X -> 1")
	 assert.Equal(raftFollwer.logs[2].Command, "Write X -> 2")
	assert.Equal(raftFollwer.logs[3].Command, "Write X -> 3")
	assert.Equal(raftFollwer.logs[4].Command, "Leader Write X -> 4")
	assert.Equal(raftFollwer.logs[5].Command, "Leader Write X -> 5")
}


/**
			0 	 1	2  3  4 5
Leader   Stub   L1 L2 L3 L4 L5 
Follower Stub   F1 F2 F3

**/
func TestRaftOverwriteLastLogFromLeader(t * testing.T) {
	raftLeader := generateRaftLaderWithYLogs(t,5)
	assert := assert.New(t)
	raftFollwer := generateRaftWithXLogs(t,3)

	// Should be [L3,L4,L5]
	raftLeaderLogs := raftLeader.getLeaderLogs(3)
	assert.Equal(len(raftLeaderLogs), 3 , "leader should have 3 logs")
	assert.Equal(raftLeaderLogs[0].Command, "Leader Write X -> 3")
	assert.Equal(raftLeaderLogs[1].Command, "Leader Write X -> 4")
	assert.Equal(raftLeaderLogs[2].Command, "Leader Write X -> 5")
	
	raftFollwer.acceptLogsFromLeader(&raftLeaderLogs, 3)
	assert.Equal(len(raftFollwer.logs), 6)
	assert.Equal(raftFollwer.logs[1].Command, "Write X -> 1")
	assert.Equal(raftFollwer.logs[2].Command, "Write X -> 2")
	assert.Equal(raftFollwer.logs[3].Command, "Leader Write X -> 3")
	assert.Equal(raftFollwer.logs[4].Command, "Leader Write X -> 4")
	assert.Equal(raftFollwer.logs[5].Command, "Leader Write X -> 5")
}

/**
			0 	 1	2  3  4 5
Leader   Stub   L1 L2 L3 L4 L5 
Follower Stub   F1 F2 F3

**/

func TestRaftOverwriteFirstLogFromLeader(t * testing.T) {
	raftLeader := generateRaftLaderWithYLogs(t,5)
	assert := assert.New(t)
	raftFollwer := generateRaftWithXLogs(t,3)
	raftLeaderLogs := raftLeader.getLeaderLogs(1)
	
	// [L1,L2,L3,L4,L5]
	assert.Equal(len(raftLeaderLogs), 5, "leader should send 5 logs")
	assert.Equal(raftLeaderLogs[0].Command, "Leader Write X -> 1")
	assert.Equal(raftLeaderLogs[1].Command, "Leader Write X -> 2")
	assert.Equal(raftLeaderLogs[2].Command, "Leader Write X -> 3")
	assert.Equal(raftLeaderLogs[3].Command, "Leader Write X -> 4")
	assert.Equal(raftLeaderLogs[4].Command, "Leader Write X -> 5")
 
		
	raftFollwer.acceptLogsFromLeader(&raftLeaderLogs, 1)
	assert.Equal(len(raftFollwer.logs), 6)
	assert.Equal(raftFollwer.logs[1].Command, "Leader Write X -> 1")
	assert.Equal(raftFollwer.logs[2].Command, "Leader Write X -> 2")
	assert.Equal(raftFollwer.logs[3].Command, "Leader Write X -> 3")
	assert.Equal(raftFollwer.logs[4].Command, "Leader Write X -> 4")
	assert.Equal(raftFollwer.logs[5].Command, "Leader Write X -> 5")
}

func generateRaftLaderWithYLogs(t * testing.T, logCount int) (*Raft) {
	cfg := make_config(t,1, false,false)
	raftInstance := cfg.rafts[0]
	raftInstance.nodeStatus = Leader 
		 
	for val :=1; val <= logCount; val++ {
		logCommand := "Leader Write X -> " + strconv.Itoa(val)
		raftInstance.appendLogEntry(logCommand)
		raftInstance.currentTerm++
	}

	return raftInstance
}

// Get a raft instance, with 4 logs not counting the stub
func generateRaftWithXLogs(t *testing.T, logCount int) (*Raft) {
	cfg := make_config(t,1,false,false)
	raftInstance := cfg.rafts[0]
	 
	for val :=1; val <= logCount; val++ {
		logCommand := "Write X -> " + strconv.Itoa(val)
		raftInstance.appendLogEntry(logCommand)
		raftInstance.currentTerm++
	} 

	return raftInstance
}

 
