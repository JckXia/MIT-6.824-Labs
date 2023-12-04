package raft 

import "testing"
// import "strconv"
import "github.com/stretchr/testify/assert"

func TestRaftLogPersistBase(t *testing.T) {
	cfg := make_config(t, 1, false, false)
	raftInstance := cfg.rafts[0]
	assert := assert.New(t)
 
	assert.Equal(len(raftInstance.logs), 1, "There should be 1 item in the log initially")
	
	raftInstance.votedFor = 2
	raftInstance.currentTerm = 3
	raftInstance.persist()

	raftState := raftInstance.persister.ReadRaftState()
	raftInstance.readPersist(raftState)

	assert.Equal(raftInstance.votedFor, 2)
	assert.Equal(raftInstance.currentTerm,3)
	// assert.Equal(raftInstance.getLastLogIdx(), 0, "Last Log Index should be 0")
} 

func TestRaftLogPersistMultiLog(t *testing.T) {
	raftInstance :=  generateRaftWithXLogs(t,4)
	assert := assert.New(t)

	// Set values 
	raftInstance.votedFor = 2
	raftInstance.currentTerm = 3
	raftInstance.persist()
 
	assert.Equal(len(raftInstance.logs), 4+1)

	raftState := raftInstance.persister.ReadRaftState()
	raftInstance.readPersist(raftState)

	
	assert.Equal(raftInstance.votedFor, 2)
	assert.Equal(raftInstance.currentTerm,3)
	assert.Equal(len(raftInstance.logs), 4+1)

}