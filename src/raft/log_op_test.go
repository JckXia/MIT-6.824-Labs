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

// Get a raft instance, with 4 logs not counting the stub
func generateRaftWithXLogs(t *testing.T, logCount int) (*Raft) {
	cfg := make_config(t,1,false,false)
	raftInstance := cfg.rafts[0]
	 
	for val :=1; val <= logCount; val++ {
		logCommand := "Write X -> " + strconv.Itoa(val)
		raftInstance.appendLogEntry(logCommand)
	} 

	return raftInstance
}

 
