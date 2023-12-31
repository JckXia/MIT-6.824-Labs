package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
 
 


type Clerk struct {
	clientId int64
	servers []*labrpc.ClientEnd
	seqNum int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) scheduleNextServer(clientId int) int {
	return (clientId + 1) % len(ck.servers)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
 
	ck := new(Clerk)
	ck.clientId = nrand()
	ck.servers = servers
	ck.seqNum = 0
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	getRequest := GetArgs{ck.clientId, ck.seqNum, key}
	getReply := GetReply{}
	serverId := 0
 	 
	for getReply.Err != OK {
		ok := ck.servers[serverId].Call("KVServer.Get", &getRequest, &getReply)
		
		if !ok {
			// Network issues/etc
		}
		serverId = ck.scheduleNextServer(serverId)
	}
	ck.seqNum++
	return getReply.Value
}

 
//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	putRequest := PutAppendArgs{ck.clientId, ck.seqNum, key, value, op}
	putResponse := PutAppendReply{}
	serverId := 0
 
	for putResponse.Err != OK {
	
		ok := ck.servers[serverId].Call("KVServer.PutAppend", &putRequest, &putResponse)
		if !ok {

		}
		serverId = ck.scheduleNextServer(serverId)
	}
	ck.seqNum++
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}


