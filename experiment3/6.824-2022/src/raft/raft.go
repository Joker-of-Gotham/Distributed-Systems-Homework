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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Raft状态
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state       State     // Follower, Candidate, or Leader
	currentTerm int       // latest term server has seen
	votedFor    int       // candidateId that received vote in current term (or -1 if none)
	lastHeard   time.Time // last time this server received a heartbeat or granted a vote
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return false
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果请求的term小于当前term，拒绝投票
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 如果请求的term大于当前term，更新term并转为Follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}

	// 检查是否已经投票给其他候选者
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 在2A中，我们不需要检查日志是否更新（因为没有日志）
	// 直接投票
	rf.votedFor = args.CandidateId
	rf.lastHeard = time.Now()
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
}

// AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderId     int // so follower can redirect clients
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int // term of prevLogIndex entry
	Entries      []interface{} // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int // leader's commitIndex
}

// AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果请求的term小于当前term，拒绝
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 如果请求的term大于等于当前term，更新term并转为Follower
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}

	// 重置lastHeard时间，表示收到了心跳
	rf.lastHeard = time.Now()

	// 在2A中，我们只处理心跳（空的Entries），总是返回成功
	reply.Term = rf.currentTerm
	reply.Success = true
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return index, term, false
	}

	// 在2A中，Start函数不需要实现日志追加
	// 只需要返回当前状态
	term = rf.currentTerm
	isLeader = true
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

// 选举超时时间范围（毫秒）
const (
	minElectionTimeout = 300 * time.Millisecond
	maxElectionTimeout = 600 * time.Millisecond
	heartbeatInterval  = 100 * time.Millisecond // 心跳间隔
	checkInterval      = 10 * time.Millisecond  // 检查间隔
)

// 生成随机的选举超时时间
func randomElectionTimeout() time.Duration {
	return minElectionTimeout + time.Duration(rand.Int63n(int64(maxElectionTimeout-minElectionTimeout)))
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(checkInterval)

		rf.mu.Lock()
		// 如果是Leader，不需要检查选举超时
		if rf.state == Leader {
			rf.mu.Unlock()
			continue
		}

		// 检查是否超时
		elapsed := time.Since(rf.lastHeard)
		electionTimeout := randomElectionTimeout()
		if elapsed >= electionTimeout {
			// 开始选举
			rf.state = Candidate
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.lastHeard = time.Now()

			// 准备RequestVote参数
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: 0, // 2A中没有日志，设为0
				LastLogTerm:  0, // 2A中没有日志，设为0
			}

			majority := len(rf.peers)/2 + 1
			term := rf.currentTerm
			rf.mu.Unlock()

			// 使用原子操作来统计投票
			var votes int32 = 1 // 自己投自己一票
			atomic.StoreInt32(&votes, 1)

			// 向所有其他服务器发送RequestVote RPC
			for i := range rf.peers {
				if i == rf.me {
					continue
				}

				go func(server int) {
					reply := RequestVoteReply{}
					if rf.sendRequestVote(server, &args, &reply) {
						rf.mu.Lock()
						defer rf.mu.Unlock()

						if rf.killed() {
							return
						}

						// 如果收到更新的term，转为Follower
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.state = Follower
							rf.votedFor = -1
							return
						}

						// 如果term不匹配，忽略这个回复
						if reply.Term != rf.currentTerm || rf.currentTerm != term {
							return
						}

						// 如果状态已经不是Candidate，忽略
						if rf.state != Candidate {
							return
						}

						// 如果获得投票
						if reply.VoteGranted {
							newVotes := atomic.AddInt32(&votes, 1)
							if int(newVotes) >= majority {
								// 获得多数票，成为Leader
								rf.state = Leader
								rf.votedFor = -1
							}
						}
					}
				}(i)
			}
		} else {
			rf.mu.Unlock()
		}
	}
}

// 心跳发送goroutine（仅当是Leader时）
func (rf *Raft) heartbeatTicker() {
	for !rf.killed() {
		time.Sleep(heartbeatInterval)

		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			continue
		}

		// 准备AppendEntries参数（心跳，空的Entries）
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      nil, // 空的Entries表示心跳
			LeaderCommit: 0,
		}
		rf.mu.Unlock()

		// 向所有其他服务器发送心跳
		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			go func(server int) {
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(server, &args, &reply)

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.killed() {
					return
				}

				// 如果收到更新的term，转为Follower
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
				}
			}(i)
		}
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
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastHeard = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeatTicker()

	return rf
}