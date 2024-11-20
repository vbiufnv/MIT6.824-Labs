package raft

//
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
	//	"bytes"
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State string

const (
	Follower  = "Follower"
	Candidate = "Candidate"
	Leader    = "Leader"
)

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//3A:
	state       State
	votedNum    int
	currentTerm int
	votedFor    int
	timeout     *time.Timer
	heartBeat   time.Duration

	log         []LogEntry
	commitIndex int
	lastApplied int

	//leader:
	nextIndex  []ApplyMsg
	matchIndex []ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (3A).
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term        int
	CandidateID int

	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	LeaderID    int
	CurrentTerm int
	Log         LogEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
// 投票逻辑
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("RequestVote:server: %d ,candidate:%d\n", rf.me, args.CandidateID)

	//candidate任期更大->投票
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
		rf.timeout.Reset(RandomTimeOut())
		rf.state = Follower
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		//candidate任期小 or 任期相同已投票  ->拒绝 返回自己的任期
	} else if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateID) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		//任期相同未投票->投票	更新votedFor timeout
	} else {
		rf.votedFor = args.CandidateID
		rf.timeout.Reset(RandomTimeOut())
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	}
}

// 增加日志逻辑/处理心跳
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//DPrintf("AppendEntries: server:%d, leader:%d\n", rf.me, args.LeaderID)
	//leader任期小 拒绝
	if args.CurrentTerm < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false

	} else {
		//leader任期大  更新任期和votedFor 状态 timeout
		if args.CurrentTerm > rf.currentTerm {
			rf.currentTerm = args.CurrentTerm
			rf.votedFor = -1
		}
		rf.state = Follower
		reply.Term = rf.currentTerm
		rf.timeout.Reset(RandomTimeOut())
		reply.Success = true
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//messageID := time.Now().UnixNano()
	//DPrintf("[%v]Server %v send sendRequestVote to %v in term %v with args %+v",messageID, rf.me,server,rf.currentTerm,*args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//DPrintf("[%vl Server %v received reply of RequestVote from %v with args=%+v reply=%+v ok=%v\n",messageID, rf.me, server, *args, reply, ok)
	return ok
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//messageID := time.Now().UnixNano()
	//DPrintf("[%v]Server %v send SendAppendEntries to %v in term %v with args %+v",messageID, rf.me,server,rf.currentTerm,*args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//DPrintf("[%vl Server %v received reply of AppendEntries from %v with args=%+v reply=%+v ok=%v\n",messageID, rf.me, server, *args, reply, ok)	
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for {
		// Your code here (3A)
		// Check if a leader election should be started.
		select {
		case <-rf.timeout.C: //超时逻辑
			if rf.killed() {
				return
			}

			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()

			switch state {

			case Follower: //未收到心跳包
				DPrintf("ticker:Follower %d timeout,Term:%d\n", rf.me, rf.currentTerm)
				rf.timeout.Reset(RandomTimeOut()) //重置选举超时器
				go rf.RunElection()

			case Candidate: //分票导致超时
				DPrintf("ticker:Candidate %d timeout,Term:%d\n", rf.me, rf.currentTerm)
				rf.timeout.Reset(RandomTimeOut())
				go rf.RunElection() //重新参与选举

			case Leader:
				DPrintf("ticker:Leader %d timeout,Term:%d\n", rf.me, rf.currentTerm)
			}
		}
	}
}

func (rf *Raft) RunElection() {

	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votedNum = 1
	rf.state = Candidate
	rf.mu.Unlock()

	//向所有server发送rpc
	go rf.sendVoteRpc2AllServer()

}

// Candidate发送投票请求
func (rf *Raft) sendVoteRpc2AllServer() {

	for i := 0; i < len(rf.peers); i++ {
		if rf.killed() {
			return
		}
		if i == rf.me {
			continue
		}

		if rf.state != Candidate {
			return
		}

		//起协程发送RequestVote RPC
		go rf.sendVoteRpc2Server(i) //参数
	}

}

func (rf *Raft) sendVoteRpc2Server(index int) {

	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateID: rf.me,
	}
	reply := &RequestVoteReply{}

	ok := rf.sendRequestVote(index, args, reply)

	if !ok {
		//DPrintf("Candidate %d sendRequestVote to %d failed\n", rf.me, index)
		return
	}

	if rf.state != Candidate {
		return
	}

	//投票
	rf.mu.Lock()
	if rf.state == Candidate && reply.VoteGranted { //候选身份且投票成功
		rf.votedNum++

		if rf.votedNum > len(rf.peers)/2 {
			rf.state = Leader
			rf.timeout.Stop()
		}
		rf.mu.Unlock()
		go rf.SendHeartBeat2AllServer()
		return
	}

	if reply.Term > rf.currentTerm { //放弃选举
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		if !rf.timeout.Stop() {
			<-rf.timeout.C
		}
		rf.timeout.Reset(RandomTimeOut())
		rf.mu.Unlock()
		DPrintf("Candidate %d become follower\n", rf.me)
		return
	}
	rf.mu.Unlock()
}

// 处理发送心跳
func (rf *Raft) SendHeartBeat2AllServer() {
	//对每个server发送心跳

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		if rf.killed() || rf.state != Leader {
			return
		}
		//DPrintf("Leader %d send HeartBeat to %d\n", rf.me, i)
		go rf.SendHeartBeat2Server(i)
	}
}

// 对服务器发送心跳
func (rf *Raft) SendHeartBeat2Server(index int) {

	for {
		//状态正常发送心跳
		//DPrintf("Leader %d send AppendEntries to %d\n", rf.me, index)
		if rf.state != Leader || rf.killed() {
			return
		}

		go func(index int){
			//DPrintf("Leader %d send AppendEntries to %d\n", rf.me, index)
			args := &AppendEntriesArgs{
				CurrentTerm: rf.currentTerm,
				LeaderID:    rf.me,
			}
			reply := &AppendEntriesReply{}
	
			
		ok:=rf.SendAppendEntries(index, args, reply)
		if !ok{
			return
		}
		//拒绝说明对方任期更大/log更新
		if !reply.Success {
			rf.mu.Lock()
			rf.state = Follower
			rf.votedFor = -1
			rf.currentTerm = reply.Term
			rf.timeout.Reset(RandomTimeOut())
			rf.mu.Unlock()
			return
		}
	}(index)

	time.Sleep(rf.heartBeat)
}

}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.mu = sync.Mutex{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.state = Follower
	rf.votedNum = 0
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.timeout = time.NewTimer(RandomTimeOut())
	rf.heartBeat = time.Millisecond * 100

	rf.log = []LogEntry{}
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func RandomTimeOut() time.Duration {

	delay, _ := rand.Int(rand.Reader, big.NewInt(151))
	n := delay.Int64() + 300
	duration := time.Millisecond * time.Duration(n)
	return duration
}
