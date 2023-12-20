package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	 "fmt"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	Key string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	getConsensusChan chan Op
	putConensusChan chan Op
	maxraftstate int // snapshot if log grows this big

	store map[string]string
	// Your definitions here.
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
 
 
	if !kv.isLeader() { 
		reply.Err = ErrWrongLeader
		return 
	}
		// Your code here.
	fmt.Printf("Get request received by leader %d \n", kv.me)
	kv.mu.Lock()
	
	opMsg := Op{"Get", args.Key,""}
	kv.rf.Start(opMsg)
	
	kv.mu.Unlock()

	//TODO Loop/range till uuid found?
//	getMsg := <- kv.getConsensusChan


	for {
		select {
		case getMsg := <- kv.getConsensusChan:
				kv.mu.Lock()
				//fmt.Println("Message!")
				if getMsg.Key == args.Key {
					reply.Value =  kv.store[getMsg.Key]
				}
				reply.Err = OK
				kv.mu.Unlock()
		default:
			
			return 
		}
	}

	// for getMsg := range kv.getConsensusChan {
	// 	kv.mu.Lock()
	// 	if getMsg.Key == args.Key {
	// 		reply.Value = kv.store[getMsg.Key]
	// 	}
	// 	kv.mu.Unlock()
	// }
 
	// kv.mu.Lock()
	// reply.Value = kv.store[getMsg.Key]

	// kv.mu.Unlock()
	
	fmt.Printf("Get request processed by leader %d \n", kv.me)
	reply.Err = OK
	return
}

func serializePutAppendArgs(args *PutAppendArgs) string {
	s := fmt.Sprintf("(K: %s, V: %s, OP: %s)", args.Key, args.Value, args.Op)
	return s
}

func serializeOpMsg(msg Op) string {
	s := fmt.Sprintf("(K: %s, V: %s, OpTye: %s)", msg.Key, msg.Value, msg.OpType)
	return s
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
 
 
	if !kv.isLeader() { 
		reply.Err = ErrWrongLeader
		return 
	}

	fmt.Printf("Append request %s received by leader %d \n", serializePutAppendArgs(args), kv.me)
 
	opType := args.Op 
	kv.mu.Lock()

 
	opLog := Op{opType, args.Key, args.Value}
	kv.rf.Start(opLog)
	kv.mu.Unlock()

	fmt.Println("Wait for append msg")
	// putAppendMsg := <- kv.putConensusChan

	// TODO: Not sure if this is a good idea tbh. 

	for {
		select {
		case putAppendMsg := <- kv.putConensusChan:
				kv.mu.Lock()
				fmt.Println("Message!")
				switch putAppendMsg.OpType {
					case "Put":
						kv.store[args.Key] = args.Value
					case "Append":
						kv.store[args.Key] += args.Value
				}
				reply.Err = OK
				kv.mu.Unlock()
		default:
			
			return 
		}
	}
	// for putAppendMsg := range kv.putConensusChan {
	// 	kv.mu.Lock()
	// 	switch putAppendMsg.OpType {
	// 		case "Put":
	// 			kv.store[args.Key] = args.Value
	// 		case "Append":
	// 			kv.store[args.Key] += args.Value
	// 	}
	// 	kv.mu.Unlock()
	// }

 
	
	 	
	fmt.Printf("Append request processed by leader %d \n", kv.me)
	reply.Err = OK
	return
}

func (kv *KVServer) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) readFromApplyCh() {
	for kv.killed() == false {
		appliedMsg := <- kv.rf.GetApplyCh()
		// commitedOperation := appliedMsg.Command
		commitedOpLog := appliedMsg.Command.(Op)
		// fmt.Println(operationLog.OpType)
		fmt.Printf("Msg arrived! %s\n", serializeOpMsg(commitedOpLog))
		//DebugP(dKv, "Applied msg received! %s", commitedOpLog.OpType)
		switch commitedOpLog.OpType {
			case "Get":
				kv.getConsensusChan <- commitedOpLog
			case "Put":
				kv.putConensusChan <- commitedOpLog
			case "Append":
				kv.putConensusChan <- commitedOpLog		
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.getConsensusChan = make(chan Op)
	kv.putConensusChan = make(chan Op)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.readFromApplyCh()
	return kv
}
