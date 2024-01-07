package raft

//
//	这里有些提醒
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/src/labgob"
	"6.824/src/labrpc"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
const (
	Leader    = 0
	Candidate = 2
	Follower  = 1
)
const (
	heartBeat = 1
	candidate = 2
	Log       = 3
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}
type LogEntry struct {
	LogTerm    int //日志的任期
	LogIndex   int //日志的索引
	LogCommand interface{}
	//interface{}值作为参数，那么可以为这个函数提供任何值
}
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //任期
	CandidateId  int //给哪个对等实体发消息
	LastLogIndex int //候选者的最后一个log条目索引
	LastLogTerm  int //候选者的最后一个log任期号
}
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int //任期
	VoteGranted bool
}
type AppendEntriesArgs struct {
	Term              int        //任期
	LeaderID          int        //leader id
	PrevLogIndex      int        //紧邻需要添加的新日志条目之前的那个日志条目索引
	PrevLogTerm       int        //紧邻需要添加的新日志条目之前的那个日志条目任期
	LeaderCommitIndex int        //Leader已知已提交的最高的日志条目缩影
	LeaderCommitTerm  int        //Leader已知已提交的最高的日志条目的日志任期
	Log               []LogEntry //需要保存的日志条目
}
type AppendEntriesReply struct {
	Term          int  //任期
	Success       bool //是否接收到了这个PRC消息
	ConflictIndex int
	ConflictTerm  int
}
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	status    int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm  int
	termLeaderId int
	right        map[int]bool
	channel      chan int
	//日志相关
	applymsg chan ApplyMsg
	//toPer sync.Mutex
	//appendLog chan bool
	lastApplied int        //最后一个已提交的日志索引
	commitIndex int        //还未提交的日志的第一个索引
	log         []LogEntry //日志
	//leader给其他peer维护的
	nextIndex   map[int]int //每一台服务器的下一条索引
	matchIndex  map[int]int //已经复制的索引
	killChannel chan bool
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.status = Follower
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0                  //将任期置0
	rf.channel = make(chan int, 50)     //接受通知
	rf.right = make(map[int]bool)       //投票器
	rf.killChannel = make(chan bool, 1) //投票器
	//日志相关
	rf.log = append(rf.log, LogEntry{LogTerm: 0, LogIndex: 0, LogCommand: nil})
	rf.applymsg = applyCh
	rf.lastApplied = 0
	rf.commitIndex = 1
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	rf.readPersist(persister.ReadRaftState())
	for i := 0; i < len(peers); i++ {
		if i == me {
			continue
		}
		//rf.nextIndex[i] = rf.lastApplied
		//rf.matchIndex[i] = rf.lastApplied+1
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	//rf.readPersist(persister.ReadRaftState())
	go rf.run()
	return rf
}

//goRoutine
func (rf *Raft) run() {
	select {
	case <-rf.killChannel:
		return
	default:
		for rf.Killed() == false {
			//在这里判断状态
			rf.mu.Lock()
			status := rf.status
			rf.mu.Unlock()
			if status == Leader {
				rf.doLogAppendJob()
			} else if status == candidate {
				rf.docandidateProcess()
			} else if status == Follower {
				rf.doFollower()
			}
		}
	}
}
func (rf *Raft) doFollower() {
	rand.Seed(time.Now().Unix() + int64(rf.me)*time.Now().Unix())
	base := 150 + rand.Intn(149)
	s := time.Duration(base) * time.Millisecond
	for rf.Killed() == false {
		select {
		case status := <-rf.channel:
			if status == candidate {
				//重新记时
				rf.mu.Lock()
				rf.status = Follower
				rf.mu.Unlock()
				return
			} else if status == heartBeat {
				//收到心跳包，重新记时
				rf.mu.Lock()
				rf.status = Follower
				rf.mu.Unlock()
				return
			} else if status == Log {
				rf.mu.Lock()
				rf.status = Follower
				rf.mu.Unlock()
			}
		case <-time.After(s):
			//rf.mu.Lock()
			//rf.status = candidate
			//rf.mu.Unlock()
			rf.doPreVoteProcess()
			return
		}
	}
}
func (rf *Raft) docandidateProcess() {
	rand.Seed(time.Now().Unix() + int64(rf.me)*time.Now().Unix())
	base := 150 + rand.Intn(149)
	s := time.Duration(base) * time.Millisecond
	//时间信息 取消线程信息
	rf.mu.Lock()
	rf.currentTerm++
	vote := 0
	_, ok := rf.right[rf.currentTerm]
	if ok == false {
		vote++
		rf.right[rf.currentTerm] = false
	}
	rf.persist()
	rf.mu.Unlock()
	var lock sync.Mutex
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(index int) {
			rf.mu.Lock()
			lastLogIndex := len(rf.log) - 1
			//lastLogIndex := rf.lastApplied
			args := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: rf.log[lastLogIndex].LogTerm}
			reply := &RequestVoteReply{}
			rf.mu.Unlock()
			res := rf.sendRequestVote(index, args, reply)
			if res == true {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				lock.Lock()
				defer lock.Unlock()
				if reply.VoteGranted == true && args.Term == rf.currentTerm && rf.status == candidate {
					vote++
					if vote >= len(rf.peers)/2+1 {
						rf.status = Leader
						//fmt.Println(rf.me,"现在是leader,任期是",rf.currentTerm)
						rf.nextIndex = make(map[int]int)
						rf.matchIndex = make(map[int]int)
						for j := 0; j < len(rf.peers); j++ {
							if j == rf.me {
								continue
							}
							minIndex := args.LastLogIndex + 1
							rf.log = rf.log[:minIndex]
							rf.nextIndex[j] = len(rf.log)
							rf.matchIndex[j] = 0
						}
						rf.termLeaderId = rf.me
						//rf.persist()
						rf.channel <- Leader
						return
					}
				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.status = Follower
						rf.persist()
						rf.channel <- Follower
					}
				}
			}
		}(i)
	}
	select {
	case <-rf.channel:
		return
	case <-time.After(s):
		return
	}
}

//变成了leader就需要周期性给peers发心跳包
func (rf *Raft) doPreVoteProcess() {
	rand.Seed(time.Now().Unix() + int64(rf.me)*time.Now().Unix())
	base := 150 + rand.Intn(149)
	s := time.Duration(base) * time.Millisecond
	//时间信息 取消线程信息
	vote := 1
	var lock sync.Mutex
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(index int) {
			rf.mu.Lock()
			lastLogIndex := len(rf.log) - 1
			args := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: lastLogIndex, LastLogTerm: rf.log[lastLogIndex].LogTerm}
			reply := &RequestVoteReply{}
			rf.mu.Unlock()
			res := rf.sendPreRequestVote(index, args, reply)
			if res == true {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				lock.Lock()
				defer lock.Unlock()
				if reply.VoteGranted == true && args.Term == rf.currentTerm {
					vote++
					if vote >= len(rf.peers)/2+1 {
						rf.status = candidate
						//rf.persist()
						rf.channel <- candidate
						return
					}
				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.status = Follower
						rf.persist()
						rf.channel <- Follower
					}
				}
			}
		}(i)
	}
	select {
	case <-rf.channel:
		return
	case <-time.After(s):
		return
	}
}
func (rf *Raft) doLogAppendJob() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(index int) {
			rf.mu.Lock()
			if rf.status != Leader {
				rf.mu.Unlock()
				return
			}
			args := &AppendEntriesArgs{Term: rf.currentTerm,
				LeaderID:          rf.me,
				PrevLogIndex:      rf.log[rf.nextIndex[index]-1].LogIndex,
				PrevLogTerm:       rf.log[rf.nextIndex[index]-1].LogTerm,
				LeaderCommitIndex: rf.commitIndex,
			}
			reply := &AppendEntriesReply{}
			var log []LogEntry
			LogLen := len(rf.log)
			for j := rf.nextIndex[index]; j < LogLen; j++ {
				log = append(log, rf.log[j])
			}
			args.Log = log
			rf.mu.Unlock()
			res := rf.sendLogEntries(index, args, reply)
			if res == true {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Success == true && rf.status == Leader && args.Term == rf.currentTerm {
					rf.nextIndex[index] = max(rf.nextIndex[index], LogLen)
					rf.matchIndex[index] = max(rf.matchIndex[index], LogLen-1)
					rf.checkN()
				} else if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.status = Follower
					rf.persist()
					rf.channel <- Follower
				} else {
					//if rf.nextIndex[index] >1{
					//	rf.nextIndex[index] -= 1
					//}
					if reply.ConflictIndex >= 0 && reply.ConflictIndex < len(rf.log) && reply.ConflictTerm == rf.log[reply.ConflictIndex].LogTerm {
						rf.nextIndex[index] = reply.ConflictIndex + 1
					} else {
						if rf.nextIndex[index] > 1 {
							rf.nextIndex[index] -= 1
						}
					}

				}
			}
		}(i)
	}
	select {
	case <-rf.channel:
		return
	case <-time.After(70 * time.Millisecond):
		return
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) checkN() {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	if rf.status != Leader || rf.termLeaderId != rf.me {
		return
	}
	for i := rf.lastApplied + 1; i < len(rf.log); i++ {
		vote := 1
		if rf.log[i].LogTerm != rf.currentTerm {
			continue
		}
		for j := 0; j < len(rf.peers); j++ {
			if rf.matchIndex[j] >= i {
				vote++
				if vote >= len(rf.peers)/2+1 {
					rf.commitIndex = i + 1
					break
				}
			}
		}
	}
	for i := rf.lastApplied + 1; i < rf.commitIndex; i++ {
		rf.lastApplied = i
		am := ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied].LogCommand, CommandIndex: rf.log[rf.lastApplied].LogIndex}
		rf.applymsg <- am
		//fmt.Println("Leader",rf.me,"任期",rf.currentTerm,"提交日志",am.CommandIndex,"日志任期",rf.log[am.CommandIndex])
	}
	//go rf.doLogAppendJob()
	rf.persist()
	//go rf.doLogAppendJob()
}
func (rf *Raft) sendLogEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.LogEntries", args, reply)
	return ok
}
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status == Leader && rf.termLeaderId == rf.me {
		isLeader = true
	} else {
		return index, term, false
	}
	//初始化log
	log := LogEntry{LogTerm: rf.currentTerm, LogCommand: command} //创建新的日志
	log.LogIndex = len(rf.log)
	rf.log = append(rf.log, log) //添加log
	index = log.LogIndex
	term = log.LogTerm
	rf.persist()
	rf.channel <- Log
	return index, term, isLeader
}
func (rf *Raft) LogEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//用于判断日志的
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("follower%d在任期%d接受了来自%d的日志包任期为%d\n",rf.me,rf.currentTerm,args.LeaderID,args.Term)
	if args.Term >= rf.currentTerm && args.PrevLogIndex < len(rf.log) && args.PrevLogTerm == rf.log[args.PrevLogIndex].LogTerm && args.PrevLogIndex == rf.log[args.PrevLogIndex].LogIndex {
		reply.Term = args.Term
		rf.currentTerm = args.Term
		rf.termLeaderId = args.LeaderID
		rf.status = Follower
		//fmt.Printf("follower%d在任期%d接受了来自%d的日志包长度为%d,他的日志长度为%d,lastApplied为%d,LeadercommitIndex为%d,args.PrevLogIndex为%d\n",rf.me,rf.currentTerm,args.LeaderID,len(args.Log),len(rf.log),rf.lastApplied,args.LeaderCommitIndex,args.PrevLogIndex)
		last := args.PrevLogIndex
		for i := 0; i < len(args.Log); i++ {
			temp := last + i + 1
			if temp < len(rf.log) {
				if rf.lastApplied < temp {
					rf.log[temp] = args.Log[i]
				}
			} else {
				rf.log = append(rf.log, args.Log[i])
			}
		}
		if args.PrevLogIndex+len(args.Log)+1 < len(rf.log) {
			index := args.PrevLogIndex + len(args.Log) + 1
			if rf.log[index].LogTerm < rf.log[index-1].LogTerm {
				rf.log = rf.log[:index]
			}
		}
		//fmt.Printf("follower%d在任期%d的日志结构为%+v\n",rf.me,rf.currentTerm,rf.log)
		if args.LeaderCommitIndex > rf.commitIndex {
			rf.commitIndex = min(len(rf.log), args.LeaderCommitIndex)
			for i := rf.lastApplied + 1; i < rf.commitIndex; i++ {
				rf.lastApplied = i
				am := ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied].LogCommand, CommandIndex: rf.log[rf.lastApplied].LogIndex}
				rf.applymsg <- am
				//fmt.Println("Follower",rf.me,"任期",rf.currentTerm,"提交日志",am.CommandIndex,"日志任期",rf.log[am.CommandIndex])
			}
		}
		reply.Success = true
		rf.persist()
		rf.channel <- Log
	} else {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = rf.lastApplied
		reply.ConflictTerm = rf.log[rf.lastApplied].LogTerm
	}

}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm <= args.Term {
		reply.Term = args.Term
		_, ok := rf.right[args.Term]
		lastLog := rf.log[len(rf.log)-1]
		if ok == false && lastLog.LogTerm < args.LastLogTerm {
			//fmt.Printf("%d收到了来自%d在任期%d的投票请求,lastLog.LogTerm为%d,args.lastLogTerm为%d\n",rf.me,args.CandidateId,args.Term,lastLog.LogTerm,args.LastLogTerm)
			rf.currentTerm = args.Term
			rf.right[args.Term] = false
			minIndex := min(args.LastLogIndex+1, len(rf.log))
			rf.log = rf.log[:minIndex]
			reply.VoteGranted = true
			rf.status = Follower
			rf.persist()
			rf.channel <- candidate
		} else if ok == false && lastLog.LogTerm == args.LastLogTerm && lastLog.LogIndex <= args.LastLogIndex {
			//fmt.Printf("%d收到了来自%d在任期%d的投票请求,lastLog.LogTerm为%d,args.lastLogTerm为%d,lastLog.LogIndex为%d,args.lastLogIndex为%d\n",rf.me,args.CandidateId,args.Term,lastLog.LogTerm,args.LastLogTerm,lastLog.LogIndex,args.LastLogIndex)
			rf.currentTerm = args.Term
			rf.right[args.Term] = false
			reply.VoteGranted = true
			minIndex := min(args.LastLogIndex+1, len(rf.log))
			rf.log = rf.log[:minIndex]
			rf.status = Follower
			rf.persist()
			rf.channel <- candidate
		} else {
			rf.currentTerm = args.Term
			rf.status = Follower
			rf.persist()
			//fmt.Printf("%d收到了来自%d在任期%d的投票请求但他没有投票\n",rf.me,args.CandidateId,args.Term)
			reply.VoteGranted = false
		}
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}
func (rf *Raft) sendPreRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.PreRequestVote", args, reply)
	return ok
}
func (rf *Raft) PreRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm <= args.Term {
		reply.Term = args.Term
		lastLog := rf.log[len(rf.log)-1]
		if lastLog.LogTerm < args.LastLogTerm {
			reply.VoteGranted = true
		} else if lastLog.LogTerm == args.LastLogTerm && lastLog.LogIndex <= args.LastLogIndex {
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.me == rf.termLeaderId && rf.status == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.lastApplied)
	e.Encode(rf.log)
	e.Encode(rf.right)
	e.Encode(rf.commitIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var lastApplied int
	var log []LogEntry
	var right map[int]bool
	var commitIndex int
	d.Decode(&currentTerm)
	d.Decode(&lastApplied)
	d.Decode(&log)
	d.Decode(&right)
	d.Decode(&commitIndex)
	rf.currentTerm = currentTerm
	rf.lastApplied = lastApplied
	rf.log = log
	rf.commitIndex = commitIndex
	rf.right = right
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.killChannel <- true
	//rf.mu.Lock()
	rf.channel <- Log
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}
func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
