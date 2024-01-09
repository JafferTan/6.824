package kvraft

import (
	"6.824/src/labgob"
	"6.824/src/labrpc"
	"6.824/src/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Op        string
	ClientId  int64
	CommandId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db                 map[string]string
	applyResChannelMap map[int]chan ApplyResChannel
	clientCommandMap   map[int64]CommandRes
}
type CommandRes struct {
	CommandId       int
	applyResChannel ApplyResChannel
}

type ApplyResChannel struct {
	Err   Err
	Key   string
	Value string
	Index int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//fmt.Printf("%d节点收到了get请求\n", kv.me)
	kv.mu.Lock()
	op := Op{Key: args.Key, Op: GET, CommandId: args.CommandId, ClientId: args.ClientId}
	commandId := args.CommandId
	if lastCommand, ok := kv.clientCommandMap[args.ClientId]; ok && lastCommand.CommandId >= commandId {
		reply.Err = lastCommand.applyResChannel.Err
		reply.Value = lastCommand.applyResChannel.Value
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	//fmt.Printf("%d节点收到了将请求发向raft层\n", kv.me)
	index, _, isLeader := kv.rf.Start(op)
	//fmt.Printf("%d节点收到了将请求发向raft层完毕\n", kv.me)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	applyResChannel := make(chan ApplyResChannel, 1)
	kv.applyResChannelMap[index] = applyResChannel
	kv.mu.Unlock()
	select {
	case res := <-applyResChannel:
		reply.Err = res.Err
		reply.Value = res.Value
		//fmt.Printf("%d号节点在(index:%d)的操作%v,回复是(res:%v)\n", kv.me, index, args, res)
	case <-time.After(500 * time.Microsecond):
		reply.Err = ErrTimeout
		//fmt.Printf("%d号节点超时没有回复\n", kv.me)
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//fmt.Printf("%d节点收到了PutAppend请求\n", kv.me)
	kv.mu.Lock()
	op := Op{Key: args.Key, Op: args.Op, Value: args.Value, ClientId: args.ClientId, CommandId: args.CommandId}
	commandId := args.CommandId
	if lastCommand, ok := kv.clientCommandMap[args.ClientId]; ok && lastCommand.CommandId >= commandId {
		reply.Err = lastCommand.applyResChannel.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	//fmt.Printf("%d节点收到了将请求发向raft层\n", kv.me)
	index, _, isLeader := kv.rf.Start(op)
	//fmt.Printf("%d节点收到了将请求发向raft层完毕\n", kv.me)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	applyResChannel := make(chan ApplyResChannel, 1)
	kv.applyResChannelMap[index] = applyResChannel
	kv.mu.Unlock()
	select {
	case res := <-applyResChannel:
		//fmt.Printf("%d号节点在(index:%d)的操作%v,回复是(res:%v)\n", kv.me, index, args, res)
		reply.Err = res.Err
	case <-time.After(500 * time.Microsecond):
		reply.Err = ErrTimeout
		//fmt.Printf("%d号节点超时没有回复\n", kv.me)
	}
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

	kv.applyCh = make(chan raft.ApplyMsg, 2000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.applyResChannelMap = make(map[int]chan ApplyResChannel)
	kv.clientCommandMap = make(map[int64]CommandRes)
	go kv.readMsgFromRaft()
	return kv
}

func (kv *KVServer) readMsgFromRaft() {
	for !kv.rf.Killed() {
		select {
		case applyMsg := <-kv.applyCh:
			if applyMsg.CommandValid == true {
				kv.doNotifyJob(applyMsg)
				//fmt.Printf("%d节点收到了applyMsg:%v\n", kv.me, applyMsg)
			}
		}
	}
	// Your code here, if desired.
}

func (kv *KVServer) doNotifyJob(applyMsg raft.ApplyMsg) {
	op, _ := applyMsg.Command.(Op)
	clientId := op.ClientId
	commandId := op.CommandId
	kv.mu.Lock()
	if lastCommand, ok := kv.clientCommandMap[clientId]; ok && lastCommand.CommandId >= commandId {
		kv.mu.Unlock()
		return
	}

	index := applyMsg.CommandIndex
	key := op.Key
	res := ApplyResChannel{}
	if op.Op == GET {
		value := kv.db[key]
		res.Key = key
		res.Value = value
		res.Err = OK
		res.Index = index
	} else if op.Op == PUT {
		kv.db[key] = op.Value
		res.Key = key
		res.Value = op.Value
		res.Err = OK
		res.Index = index
	} else if op.Op == APPEND {
		value := kv.db[key]
		newValue := value + op.Value
		kv.db[key] = newValue

		res.Key = key
		res.Value = newValue
		res.Err = OK
		res.Index = index
	}
	if chanRes, ok := kv.applyResChannelMap[index]; ok {
		chanRes <- res
	}
	kv.clientCommandMap[op.ClientId] = CommandRes{CommandId: commandId, applyResChannel: res}
	kv.mu.Unlock()
}
