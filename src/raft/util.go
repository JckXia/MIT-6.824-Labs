package raft

import (
	"log"
	"math/rand"
	"time"
	"os"
	"strconv"
	"fmt"
)


type logTopic string
const (
	dReplica logTopic = "REPL"
	dCommit logTopic = "CMIT"
	dLeader logTopic = "LEAD"
	dPersist logTopic = "PERS"
	dTimer logTopic = "TIMR"
	dElection logTopic = "ELEC"
	dVote logTopic = "VOTE"
	dDrop logTopic = "DROP"
	dWarn logTopic = "WARN"
)

// Debugging
const DebugF = false
var debugStart time.Time 
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()
	
	//log.SetFlags(0)
}

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error 
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v",v)
		}
	}
	return level
}


func DebugP(topic logTopic, format string, a...interface{}) {
	if debugVerbosity >= 1 && topic == dCommit {
		log.SetFlags(0)
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format 
		log.Printf(format, a...)
	}
}

func serializeLogContents(logs []Log) string {
	var result string = ""
	for i := 0; i < len(logs); i++ {
		logContent := fmt.Sprintf("[(T: %d, C: %s)]", logs[i].CommandTerm, logs[i].Command)
		if result == "" {
			result = logContent
		} else {
			result = fmt.Sprintf("%s %s", result, logContent)
		}
	}
	return result
}


func serializeLog(log Log) string {
	logContent := fmt.Sprintf("[(T: %d, C: %s)]", log.CommandTerm, log.Command)
	return logContent
}

func DPrintf(debugLevels string, format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if DebugF && (debugLevels == LOG_LEVEL_PERSISTENCE ) {
		log.Printf(format, a...)
	}

	if debugLevels == LOG_LEVEL_WARN{
		log.Printf(format, a...)
	}

	return
}

func DebugPrintf(format string, a ...interface{}) {
	log.Printf(format, a...)
} 

func RandRange(min, max int) int {
    if min >= max {
        return min
    }
    rand.Seed(time.Now().UnixNano())
    return min + rand.Intn(max-min)
}

func GetServerState(serverState int) (string){
	if serverState == Follower {
		return "Follower"
	}

	if serverState == Candidate {
		return "Candidate"
	}

	return "Leader"
}

func min(a int, b int) int {
	if (a > b ) {
		return b
	}
	return a
}

func max(a int, b int) int {
	if a > b {
		return a 
	}
	return b
}