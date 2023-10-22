package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = true
 
 
func DPrintf(debugLevels string, format string, a ...interface{}) (n int, err error) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if Debug && debugLevels == LOG_LEVEL_REPLICATION {
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