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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
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

// Log entry structure
type LogEntry struct {
	Term    int         // term when entry was received by leader
	Command interface{} // command for state machine
}

// Raft state
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

	// Persistent state on all servers (updated on stable storage before responding to RPCs)
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term (or -1 if none)
	log         []LogEntry // log entries; first index is 1

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// Volatile state on leaders (reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send
	matchIndex []int // for each server, index of highest log entry known to be replicated

	// Additional state
	state     State     // current role
	lastHeard time.Time // last time received heartbeat or granted vote
	applyCh   chan ApplyMsg
	applyCond *sync.Cond // condition variable for apply
}

// Timing constants
const (
	minElectionTimeout = 250 * time.Millisecond
	maxElectionTimeout = 400 * time.Millisecond
	heartbeatInterval  = 100 * time.Millisecond
)

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// Caller must hold the lock.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		// error decoding
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
}

// A service wants to switch to snapshot. Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Not needed for 2A/2B/2C
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Not needed for 2A/2B/2C
}

// RequestVote RPC arguments structure
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// RequestVote RPC reply structure
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// Get the last log index
func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

// Get the last log term
func (rf *Raft) lastLogTerm() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Term
}

// Check if candidate's log is at least as up-to-date as receiver's log
func (rf *Raft) isLogUpToDate(lastLogIndex, lastLogTerm int) bool {
	myLastTerm := rf.lastLogTerm()
	myLastIndex := rf.lastLogIndex()

	if lastLogTerm != myLastTerm {
		return lastLogTerm > myLastTerm
	}
	return lastLogIndex >= myLastIndex
}

// RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// If RPC request contains term > currentTerm, update currentTerm and convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
	}

	reply.Term = rf.currentTerm

	// Check if we can vote for this candidate
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int        // leader's commitIndex
}

// AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	// For fast backup optimization
	XTerm  int // term of conflicting entry, or -1 if log is too short
	XIndex int // first index of XTerm, or length of log if XTerm == -1
	XLen   int // length of follower's log
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// If RPC request contains term >= currentTerm, update term and convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	rf.state = Follower
	rf.lastHeard = time.Now()

	reply.Term = rf.currentTerm

	// Check if log contains an entry at prevLogIndex with prevLogTerm
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > rf.lastLogIndex() {
			// Log is too short
			reply.Success = false
			reply.XTerm = -1
			reply.XIndex = rf.lastLogIndex() + 1
			reply.XLen = len(rf.log)
			return
		}
		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			// Conflicting entry
			reply.Success = false
			reply.XTerm = rf.log[args.PrevLogIndex].Term
			// Find the first index of XTerm
			reply.XIndex = args.PrevLogIndex
			for reply.XIndex > 1 && rf.log[reply.XIndex-1].Term == reply.XTerm {
				reply.XIndex--
			}
			reply.XLen = len(rf.log)
			return
		}
	}

	// If an existing entry conflicts with a new one, delete it and all that follow it
	// Append any new entries not already in the log
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx < len(rf.log) {
			if rf.log[idx].Term != entry.Term {
				// Conflict: delete this and all following entries
				rf.log = rf.log[:idx]
				rf.log = append(rf.log, args.Entries[i:]...)
				rf.persist()
				break
			}
		} else {
			// Append remaining entries
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
	}

	// Update commitIndex
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
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Start agreement on a new log entry
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	// Append entry to local log
	entry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	rf.persist()

	index := rf.lastLogIndex()
	term := rf.currentTerm

	// Immediately start replicating
	go rf.broadcastAppendEntries()

	return index, term, true
}

// Kill the Raft instance
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Generate random election timeout
func randomElectionTimeout() time.Duration {
	return minElectionTimeout + time.Duration(rand.Int63n(int64(maxElectionTimeout-minElectionTimeout)))
}

// The ticker goroutine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()
		if rf.state == Leader {
			rf.mu.Unlock()
			continue
		}

		elapsed := time.Since(rf.lastHeard)
		electionTimeout := randomElectionTimeout()
		if elapsed >= electionTimeout {
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}

// Start a new election (caller must hold the lock)
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

				// Check if still candidate and term matches
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

// Become leader (caller must hold the lock)
func (rf *Raft) becomeLeader() {
	rf.state = Leader

	// Initialize nextIndex and matchIndex
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
		rf.matchIndex[i] = 0
	}

	// Send initial heartbeat immediately
	go rf.broadcastAppendEntries()
}

// Heartbeat ticker for leader
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

// Broadcast AppendEntries to all peers
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

// Send AppendEntries to a single peer
func (rf *Raft) sendAppendEntriesToPeer(server int, term int) {
	rf.mu.Lock()
	if rf.state != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}

	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := 0
	if prevLogIndex > 0 && prevLogIndex < len(rf.log) {
		prevLogTerm = rf.log[prevLogIndex].Term
	}

	// Copy entries to send
	entries := make([]LogEntry, 0)
	if rf.nextIndex[server] <= rf.lastLogIndex() {
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

		// Check if still leader and term matches
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
			// Update nextIndex and matchIndex
			newNextIndex := args.PrevLogIndex + len(args.Entries) + 1
			newMatchIndex := args.PrevLogIndex + len(args.Entries)
			if newNextIndex > rf.nextIndex[server] {
				rf.nextIndex[server] = newNextIndex
			}
			if newMatchIndex > rf.matchIndex[server] {
				rf.matchIndex[server] = newMatchIndex
			}

			// Check if we can advance commitIndex
			rf.advanceCommitIndex()
		} else {
			// Fast backup: use XTerm, XIndex, XLen
			if reply.XTerm == -1 {
				// Log is too short
				rf.nextIndex[server] = reply.XLen
			} else {
				// Find if leader has XTerm
				found := false
				for idx := args.PrevLogIndex; idx >= 1; idx-- {
					if rf.log[idx].Term == reply.XTerm {
						rf.nextIndex[server] = idx + 1
						found = true
						break
					}
					if rf.log[idx].Term < reply.XTerm {
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

// Advance commitIndex if possible (caller must hold the lock)
func (rf *Raft) advanceCommitIndex() {
	for n := rf.lastLogIndex(); n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			continue
		}

		count := 1 // count self
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

// Apply committed entries to state machine
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

		// Apply entries from lastApplied+1 to commitIndex
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.log[lastApplied+1:commitIndex+1])
		rf.mu.Unlock()

		for i, entry := range entries {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: lastApplied + 1 + i,
			}
			rf.applyCh <- msg
		}

		rf.mu.Lock()
		if commitIndex > rf.lastApplied {
			rf.lastApplied = commitIndex
		}
		rf.mu.Unlock()
	}
}

// Make creates a new Raft server instance
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Initialize state
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1) // log[0] is a dummy entry, real log starts at index 1
	rf.log[0] = LogEntry{Term: 0, Command: nil}

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastHeard = time.Now()

	rf.applyCond = sync.NewCond(&rf.mu)

	// Initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Start background goroutines
	go rf.ticker()
	go rf.heartbeatTicker()
	go rf.applier()

	return rf
}
