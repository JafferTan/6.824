package kvraft

import (
	"6.824/src/labrpc"
	"fmt"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId      int
	clientId      int64
	lastCommandId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderId = 0
	ck.clientId = nrand()
	ck.lastCommandId = 0

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

	i := ck.leaderId
	commandId := ck.lastCommandId + 1
	args := GetArgs{Key: key, ClientId: ck.clientId, CommandId: commandId}
	reply := GetReply{}
	for {
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			i = (i + 1) % len(ck.servers)
			fmt.Printf("客户端get选取下一个节点%d进行rpc\n", i)
			continue
		}
		ck.leaderId = i
		ck.lastCommandId = commandId
		if reply.Err == ErrNoKey {
			return ""
		}
		return reply.Value
	}

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
	i := ck.leaderId
	commandId := ck.lastCommandId + 1
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		CommandId: commandId,
		ClientId:  ck.clientId,
	}
	reply := PutAppendReply{}
	for {
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			i = (i + 1) % len(ck.servers)
			fmt.Printf("客户端put选取下一个节点%d进行rpc\n", i)
			continue
		}
		ck.leaderId = i
		ck.lastCommandId = commandId
		return
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
