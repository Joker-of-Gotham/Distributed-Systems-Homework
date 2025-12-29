package raft

//
// Raft consensus module (Lab 2A/2B/2C only).
//
//

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// ApplyMsg is sent by Raft to the service (or tester) when a log entry commits.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D (snapshots). Kept for compatibility with the original tester code.
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Log entry.
type LogEntry struct {
	Term    int
	Command interface{}
}

// Raft role.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int
	dead      int32

	// Persistent state on all servers.
	currentTerm int
	votedFor    int
	log         []LogEntry // log[0] is a dummy entry; real log starts at index 1

	// Volatile state on all servers.
	commitIndex int
	lastApplied int

	// Volatile state on leaders.
	nextIndex  []int
	matchIndex []int

	// Additional state.
	state     State
	lastHeard time.Time
	applyCh   chan ApplyMsg
	applyCond *sync.Cond
}

// Timing constants (tuned to satisfy the tester's constraints).
const (
	minElectionTimeout = 200 * time.Millisecond
	maxElectionTimeout = 350 * time.Millisecond
	heartbeatInterval  = 100 * time.Millisecond
)

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) lastLogTerm() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getLogTerm(index int) int {
	if index < 0 || index >= len(rf.log) {
		return -1
	}
	return rf.log[index].Term
}

// GetState returns currentTerm and whether this server believes it is leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// persist saves Raft's persistent state.
// Caller must hold rf.mu.
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.encodeState())
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	return w.Bytes()
}

// readPersist restores previously persisted state.
// Caller should NOT assume any volatile state is restored.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var log []LogEntry
	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		return
	}
	rf.currentTerm = term
	rf.votedFor = votedFor
	rf.log = log
}

// CondInstallSnapshot is kept for compatibility with the original Lab 2 tester.
// This implementation does not coordinate snapshots with log application; the
// tester's advice allows returning true.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

// Snapshot is kept for compatibility with the original Lab 2 tester and upper layers.
// This codebase doesn't implement log compaction; we only persist the snapshot bytes
// together with Raft state so that callers compiling against the API succeed.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
}

// RequestVote RPC arguments.
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVote RPC reply.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) isLogUpToDate(lastLogIndex, lastLogTerm int) bool {
	myLastTerm := rf.lastLogTerm()
	myLastIndex := rf.lastLogIndex()
	if lastLogTerm != myLastTerm {
		return lastLogTerm > myLastTerm
	}
	return lastLogIndex >= myLastIndex
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
	}

	reply.Term = rf.currentTerm

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.votedFor = args.CandidateId
		rf.lastHeard = time.Now()
		rf.persist()
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

// AppendEntries RPC arguments.
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntries RPC reply.
type AppendEntriesReply struct {
	Term    int
	Success bool

	// Fast backup optimization (2C).
	XTerm  int
	XIndex int
	XLen   int
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	rf.state = Follower
	rf.lastHeard = time.Now()

	reply.Term = rf.currentTerm

	// If log doesn't contain entry at PrevLogIndex, it's too short.
	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		reply.XTerm = -1
		reply.XIndex = len(rf.log)
		reply.XLen = len(rf.log)
		return
	}

	// If term doesn't match at PrevLogIndex, conflict.
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		xIndex := args.PrevLogIndex
		for xIndex > 1 && rf.log[xIndex-1].Term == reply.XTerm {
			xIndex--
		}
		reply.XIndex = xIndex
		reply.XLen = len(rf.log)
		return
	}

	// Append new entries, deleting conflicts.
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx < len(rf.log) {
			if rf.log[idx].Term != entry.Term {
				rf.log = rf.log[:idx]
				rf.log = append(rf.log, args.Entries[i:]...)
				rf.persist()
				break
			}
		} else {
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
	}

	// Update commitIndex.
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < rf.lastLogIndex() {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.lastLogIndex()
		}
		rf.applyCond.Signal()
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// Start agreement on a new log entry.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	entry := LogEntry{Term: rf.currentTerm, Command: command}
	rf.log = append(rf.log, entry)
	rf.persist()

	index := rf.lastLogIndex()
	term := rf.currentTerm

	// Immediately start replication.
	go rf.broadcastAppendEntries()

	return index, term, true
}

// Kill is called by the tester to shut down a Raft instance.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func randomElectionTimeout() time.Duration {
	return minElectionTimeout + time.Duration(rand.Int63n(int64(maxElectionTimeout-minElectionTimeout)))
}

// ticker starts elections if no heartbeats have been received.
func (rf *Raft) ticker() {
	for !rf.killed() {
		timeout := randomElectionTimeout()
		time.Sleep(timeout)

		rf.mu.Lock()
		if rf.state == Leader {
			rf.mu.Unlock()
			continue
		}
		elapsed := time.Since(rf.lastHeard)
		if elapsed >= timeout {
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}

// startElection begins an election (caller must hold rf.mu).
func (rf *Raft) startElection() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastHeard = time.Now()
	rf.persist()

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.lastLogTerm(),
	}

	term := rf.currentTerm
	votes := 1
	majority := len(rf.peers)/2 + 1

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
				if rf.state != Candidate || rf.currentTerm != term {
					return
				}
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.votedFor = -1
					rf.persist()
					return
				}
				if reply.VoteGranted {
					votes++
					if votes >= majority && rf.state == Candidate {
						rf.becomeLeader()
					}
				}
			}
		}(i)
	}
}

// becomeLeader transitions to leader (caller must hold rf.mu).
func (rf *Raft) becomeLeader() {
	rf.state = Leader

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	next := rf.lastLogIndex() + 1
	for i := range rf.peers {
		rf.nextIndex[i] = next
		rf.matchIndex[i] = 0
	}

	// Send initial heartbeat immediately.
	go rf.broadcastAppendEntries()
}

// heartbeatTicker periodically triggers replication/heartbeats.
func (rf *Raft) heartbeatTicker() {
	for !rf.killed() {
		time.Sleep(heartbeatInterval)
		rf.mu.Lock()
		if rf.state == Leader {
			go rf.broadcastAppendEntries()
		}
		rf.mu.Unlock()
	}
}

// broadcastAppendEntries sends AppendEntries to all peers.
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntriesToPeer(i, term)
	}
}

// sendAppendEntriesToPeer sends AppendEntries to a single follower.
func (rf *Raft) sendAppendEntriesToPeer(server int, term int) {
	rf.mu.Lock()
	if rf.state != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}

	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := rf.getLogTerm(prevLogIndex)
	if prevLogTerm < 0 {
		prevLogTerm = 0
	}

	entries := make([]LogEntry, 0)
	if rf.nextIndex[server] >= 0 && rf.nextIndex[server] < len(rf.log) {
		entries = append(entries, rf.log[rf.nextIndex[server]:]...)
	}

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	if rf.sendAppendEntries(server, &args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.killed() {
			return
		}
		if rf.state != Leader || rf.currentTerm != term {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
			return
		}

		if reply.Success {
			newNext := args.PrevLogIndex + len(args.Entries) + 1
			newMatch := args.PrevLogIndex + len(args.Entries)
			if newNext > rf.nextIndex[server] {
				rf.nextIndex[server] = newNext
			}
			if newMatch > rf.matchIndex[server] {
				rf.matchIndex[server] = newMatch
			}
			rf.advanceCommitIndex()
		} else {
			if reply.XTerm == -1 {
				rf.nextIndex[server] = reply.XIndex
			} else {
				found := false
				for idx := rf.lastLogIndex(); idx >= 1; idx-- {
					if rf.getLogTerm(idx) == reply.XTerm {
						rf.nextIndex[server] = idx + 1
						found = true
						break
					}
				}
				if !found {
					rf.nextIndex[server] = reply.XIndex
				}
			}
			if rf.nextIndex[server] < 1 {
				rf.nextIndex[server] = 1
			}
		}
	}
}

// advanceCommitIndex advances commitIndex if a majority has replicated an entry.
// Caller must hold rf.mu.
func (rf *Raft) advanceCommitIndex() {
	for n := rf.lastLogIndex(); n > rf.commitIndex; n-- {
		if rf.getLogTerm(n) != rf.currentTerm {
			continue
		}
		count := 1 // self
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			rf.applyCond.Signal()
			break
		}
	}
}

// applier applies committed entries to the service.
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
			if rf.killed() {
				rf.mu.Unlock()
				return
			}
		}

		nextIndex := rf.lastApplied + 1
		if nextIndex > rf.commitIndex {
			rf.mu.Unlock()
			continue
		}
		if nextIndex <= 0 || nextIndex >= len(rf.log) {
			rf.mu.Unlock()
			continue
		}

		entry := rf.log[nextIndex]
		rf.mu.Unlock()

		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: nextIndex,
		}

		rf.mu.Lock()
		if nextIndex > rf.lastApplied {
			rf.lastApplied = nextIndex
		}
		rf.mu.Unlock()
	}
}

// Make creates a new Raft server instance.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{Term: 0, Command: nil}

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastHeard = time.Now()

	rf.applyCond = sync.NewCond(&rf.mu)

	rf.readPersist(persister.ReadRaftState())

	go rf.ticker()
	go rf.heartbeatTicker()
	go rf.applier()

	return rf
}


