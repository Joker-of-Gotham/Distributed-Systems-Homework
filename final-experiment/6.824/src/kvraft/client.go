package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"

	"6.824/labrpc"
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	clientId int64
	leaderId int32 // cache the leader id
	seqNum   int64 // monotonically increasing sequence number
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
	ck.clientId = nrand()
	ck.leaderId = 0
	ck.seqNum = 0
	return ck
}

// Get fetches the current value for a key.
// Returns "" if the key does not exist.
// Keeps trying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	requestId := atomic.AddInt64(&ck.seqNum, 1)
	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		RequestId: requestId,
	}

	for {
		leaderId := atomic.LoadInt32(&ck.leaderId)
		reply := GetReply{}
		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)

		if ok {
			switch reply.Err {
			case OK:
				return reply.Value
			case ErrNoKey:
				return ""
			case ErrWrongLeader, ErrTimeout:
				// Try next server
			}
		}

		// Try next server
		atomic.StoreInt32(&ck.leaderId, (leaderId+1)%int32(len(ck.servers)))
	}
}

// PutAppend sends a Put or Append request to the server.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	requestId := atomic.AddInt64(&ck.seqNum, 1)
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		RequestId: requestId,
	}

	for {
		leaderId := atomic.LoadInt32(&ck.leaderId)
		reply := PutAppendReply{}
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)

		if ok {
			switch reply.Err {
			case OK:
				return
			case ErrWrongLeader, ErrTimeout:
				// Try next server
			}
		}

		// Try next server
		atomic.StoreInt32(&ck.leaderId, (leaderId+1)%int32(len(ck.servers)))
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
