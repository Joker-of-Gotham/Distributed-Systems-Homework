package kvraft

import (
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const ExecuteTimeout = 500 * time.Millisecond

type Op struct {
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      string // "Get", "Put", or "Append"
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
}

// Result of executing an operation
type OpResult struct {
	Err   Err
	Value string
}

// Pending request waiting for Raft to apply
type PendingRequest struct {
	index   int
	term    int
	resultC chan OpResult
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // log size threshold

	// Key-value store
	kvStore map[string]string

	// Track latest processed request for each client (for deduplication)
	lastApplied map[int64]int64

	// Store results for each client's last request
	lastResults map[int64]OpResult

	// Pending requests waiting for Raft to apply
	pendingReqs map[int]*PendingRequest
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Type:      "Get",
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	result := kv.executeOp(op)
	reply.Err = result.Err
	reply.Value = result.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	result := kv.executeOp(op)
	reply.Err = result.Err
}

func (kv *KVServer) executeOp(op Op) OpResult {
	kv.mu.Lock()

	// Check for duplicate request (only for Put/Append)
	if op.Type != "Get" {
		if lastReqId, ok := kv.lastApplied[op.ClientId]; ok && lastReqId >= op.RequestId {
			result := kv.lastResults[op.ClientId]
			kv.mu.Unlock()
			return result
		}
	}
	kv.mu.Unlock()

	// Submit to Raft
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		return OpResult{Err: ErrWrongLeader}
	}

	// Create pending request
	resultC := make(chan OpResult, 1)
	kv.mu.Lock()
	kv.pendingReqs[index] = &PendingRequest{
		index:   index,
		term:    term,
		resultC: resultC,
	}
	kv.mu.Unlock()

	// Wait for result or timeout
	select {
	case result := <-resultC:
		return result
	case <-time.After(ExecuteTimeout):
		kv.mu.Lock()
		delete(kv.pendingReqs, index)
		kv.mu.Unlock()
		return OpResult{Err: ErrTimeout}
	}
}

// Apply committed entries from Raft
func (kv *KVServer) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh

		if msg.CommandValid {
			kv.applyCommand(msg)
		}
	}
}

func (kv *KVServer) applyCommand(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := msg.Command.(Op)
	index := msg.CommandIndex

	var result OpResult

	// Check if this request was already applied (deduplication)
	if op.Type != "Get" {
		if lastReqId, ok := kv.lastApplied[op.ClientId]; ok && lastReqId >= op.RequestId {
			// Duplicate request, use cached result
			result = kv.lastResults[op.ClientId]
		} else {
			// Execute the operation
			result = kv.applyOp(op)
			kv.lastApplied[op.ClientId] = op.RequestId
			kv.lastResults[op.ClientId] = result
		}
	} else {
		// Get requests always execute
		result = kv.applyOp(op)
	}

	// Notify pending request if any
	if pending, ok := kv.pendingReqs[index]; ok {
		// Check if we're still the leader for this term
		currentTerm, isLeader := kv.rf.GetState()
		if isLeader && currentTerm == pending.term {
			pending.resultC <- result
		} else {
			pending.resultC <- OpResult{Err: ErrWrongLeader}
		}
		delete(kv.pendingReqs, index)
	}
}

func (kv *KVServer) applyOp(op Op) OpResult {
	switch op.Type {
	case "Get":
		if value, ok := kv.kvStore[op.Key]; ok {
			return OpResult{Err: OK, Value: value}
		}
		return OpResult{Err: ErrNoKey, Value: ""}
	case "Put":
		kv.kvStore[op.Key] = op.Value
		return OpResult{Err: OK}
	case "Append":
		kv.kvStore[op.Key] += op.Value
		return OpResult{Err: OK}
	}
	return OpResult{Err: OK}
}

// Kill the KVServer instance
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer creates and starts a new KVServer
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// Call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.kvStore = make(map[string]string)
	kv.lastApplied = make(map[int64]int64)
	kv.lastResults = make(map[int64]OpResult)
	kv.pendingReqs = make(map[int]*PendingRequest)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Start the applier goroutine
	go kv.applier()

	return kv
}
