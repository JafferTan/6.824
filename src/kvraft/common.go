package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64
	CommandId int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	ClientId  int64
	CommandId int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
