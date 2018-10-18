package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// Op what
type Op struct {
	// Your definitions here.

	// metadata
	OpID   int64
	LastID int64
	Type   string
	View   int
	Ck     int64
	GID    int64

	// data
	Update  map[string]string
	Key     string
	Value   string
	Config  shardmaster.Config
	Shards  []int
	Clients map[int64]int
}

func printOp(op Op) string {
	switch op.Type {
	case "Get":
		return "GET " + op.Key
	case "Put":
		return "PUT " + op.Value + " INTO " + op.Key
	case "Append":
		return "APPEND " + op.Value + " INTO " + op.Key
	case "Update":
		return fmt.Sprintf("UPDATE %d FROM %d", op.View, op.GID)
	case "Reconfig":
		return fmt.Sprintf("RECONFIG %d", op.Config.Num)
	}
	return "ERROR"
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	seq         int
	current     shardmaster.Config
	data        map[string]string
	seenOpID    map[int64]int
	hasShard    [shardmaster.NShards]bool
	outstanding map[int64]bool
}

func copyMap(m map[int64]int) map[int64]int {
	if m == nil {
		return nil
	}

	newm := make(map[int64]int)

	for k, v := range m {
		// negative indicates copied
		if v > 0 {
			newm[k] = -v
		} else {
			newm[k] = v
		}
	}

	return newm
}

func (kv *ShardKV) getOp(seq int) Op {
	to := 10 * time.Millisecond
	for {
		status, val := kv.px.Status(seq)
		if status == paxos.Decided {
			fmt.Printf("%d PAXOS ON %d-%d --- %d %s\n", kv.current.Num, kv.gid, kv.me, seq, printOp(val.(Op)))
			return val.(Op)
		}

		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (kv *ShardKV) handleReq(id int64, shard int, op Op) Err {
	for !kv.isdead() {
		// are we assigned shard?
		if kv.current.Shards[shard] == kv.gid {

			// do we have shard?
			if kv.hasShard[shard] {

				// have we seen opID before?
				if kv.seenOpID[id] == 0 {

					kv.seq++
					seq := kv.seq

					kv.px.Start(seq, op)
					xop := kv.getOp(seq)
					kv.process(xop, seq, op.Ck)

					if xop.OpID == id {
						return OK
					}
				} else {
					return OK
				}
			} else {
				fmt.Println(kv.current.Shards)
				return ErrTryAgain
			}
		} else {
			return ErrWrongGroup
		}
	}
	return "Dead"
}

func (kv *ShardKV) process(op Op, seq int, server int64) {
	if op.Type == "Reconfig" {
		kv.doReconfig(op)
		return
	}
	if op.Type == "Update" {
		kv.doUpdate(op)
		return
	}

	kv.seenOpID[op.OpID] = seq

	if op.Type == "Append" {
		kv.data[op.Key] += op.Value
	} else if op.Type == "Put" {
		kv.data[op.Key] = op.Value
	}

	// did we get a request from op's server?
	if op.Ck == server {
		lastSeq, ok := kv.seenOpID[op.LastID]

		if !ok {
			return
		}
		// only call done if not copied op
		if lastSeq > 0 {
			kv.px.Done(lastSeq)
		}
		delete(kv.seenOpID, op.LastID)
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{OpID: args.OpID, Type: "Get", Key: args.Key, Ck: args.Ck, LastID: args.LastID}
	err := kv.handleReq(args.OpID, key2shard(args.Key), op)

	if err == OK {
		val, ok := kv.data[args.Key]
		if !ok {
			reply.Err = ErrNoKey
		} else {
			reply.Err = OK
			reply.Value = val
		}
	} else {
		reply.Err = err
	}
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var op Op
	if args.Op == "Put" {
		op = Op{OpID: args.OpID, Type: "Put", Key: args.Key, Value: args.Value, Ck: args.Ck, LastID: args.LastID}
	} else {
		op = Op{OpID: args.OpID, Type: "Append", Key: args.Key, Value: args.Value, Ck: args.Ck, LastID: args.LastID}
	}
	reply.Err = kv.handleReq(args.OpID, key2shard(args.Key), op)

	return nil
}

func (kv *ShardKV) doUpdate(op Op) {
	fmt.Printf("%d UPDATING %d-%d with %#v\n", kv.current.Num, kv.gid, kv.me, op.Shards)
	if kv.current.Num == op.View {
		if op.Update != nil {
			for k, v := range op.Update {
				kv.data[k] = v
			}
		}
		for _, i := range op.Shards {
			kv.hasShard[i] = true
		}
		for k, v := range op.Clients {
			kv.seenOpID[k] = v
		}
		delete(kv.outstanding, op.GID)
	}
}

func (kv *ShardKV) Update(args *UpdateArgs, reply *UpdateReply) error {
	// send update to self
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{Type: "Update", Update: args.Data, View: args.View, GID: args.From, Shards: args.Shards, Clients: args.Clients}
	for !kv.isdead() {
		if op.View == kv.current.Num {
			if kv.outstanding[args.From] == false {
				reply.Err = OK
				return nil
			}
			kv.seq++
			seq := kv.seq

			kv.px.Start(seq, op)
			xop := kv.getOp(seq)
			kv.process(xop, seq, 0)

			if xop.Type == "Update" && xop.View == op.View && xop.GID == op.GID {
				reply.Err = OK
				return nil
			}
		} else if op.View > kv.current.Num {
			reply.Err = ErrTryAgain
			return nil
		} else {
			reply.Err = OK
			return nil
		}
	}
	return nil
}

// send shards to other groups
func (kv *ShardKV) sendUpdates(servers []string, msg map[string]string, shards []int,
	clients map[int64]int, view int, gid int64) {
	args := &UpdateArgs{View: view, Data: msg, Shards: shards, From: kv.gid, Clients: clients}
	fmt.Printf("%d SENDING FROM %d-%d TO %d --- %#v\n", view, kv.gid, kv.me, gid, shards)

	for {
		for _, srv := range servers {
			var reply UpdateReply
			ok := call(srv, "ShardKV.Update", args, &reply)
			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) doReconfig(op Op) {
	if kv.current.Num != op.Config.Num-1 {
		return
	}

	msg := make(map[int64]map[string]string)
	shards := make(map[int64][]int)

	for i := 0; i < shardmaster.NShards; i++ {
		// find shards to send
		if (op.Config.Shards[i] != kv.gid) && (kv.current.Shards[i] == kv.gid) {
			kv.hasShard[i] = false
			shards[op.Config.Shards[i]] = append(shards[op.Config.Shards[i]], i)
		}

		// set outstanding flag
		if (op.Config.Shards[i] == kv.gid) && (kv.current.Shards[i] != kv.gid) && op.Config.Num != 1 {
			kv.outstanding[kv.current.Shards[i]] = true
		}

		// first group
		if op.Config.Num == 1 && op.Config.Shards[0] == kv.gid {
			kv.hasShard[i] = true
		}

	}

	for k, v := range kv.data {
		// write messages
		shard := key2shard(k)
		if (op.Config.Shards[key2shard(k)] != kv.gid) && (kv.current.Shards[key2shard(k)] == kv.gid) {
			if msg[op.Config.Shards[shard]] == nil {
				msg[op.Config.Shards[shard]] = make(map[string]string)
			}
			msg[op.Config.Shards[shard]][k] = v
		}
	}

	kv.current = op.Config
	fmt.Printf("%d RECONFIGGED  %d-%d --- %#v\n", op.Config.Num, kv.gid, kv.me, op.Config.Shards)

	for gid, servers := range op.Config.Groups {
		// send updates
		if gid != kv.gid && len(shards[gid]) > 0 {
			go kv.sendUpdates(servers, msg[gid], shards[gid], copyMap(kv.seenOpID), op.Config.Num, gid)
		}
	}
}

func (kv *ShardKV) reconfigure(config shardmaster.Config) {
	op := Op{Type: "Reconfig", Config: config}
	for !kv.isdead() {
		// check if up to date before reconfiging
		if len(kv.outstanding) == 0 {
			kv.seq++
			seq := kv.seq

			kv.px.Start(seq, op)
			xop := kv.getOp(seq)
			kv.process(xop, seq, 0)

			if xop.Type == "Reconfig" && xop.Config.Num == config.Num {
				return
			}
		} else {
			// try to catch up
			status, val := kv.px.Status(kv.seq + 1)
			if status == paxos.Decided {
				kv.seq++
				fmt.Printf("%d PAXOS ON %d-%d --- %d %s\n", kv.current.Num, kv.gid, kv.me, kv.seq, printOp(val.(Op)))
				kv.process(val.(Op), kv.seq, 0)
			}
			return
		}
	}
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	nconfig := kv.sm.Query(-1)
	if nconfig.Num > kv.current.Num {
		if nconfig.Num > kv.current.Num+1 {
			nconfig = kv.sm.Query(kv.current.Num + 1)
		}
		kv.reconfigure(nconfig)
	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().

	kv.data = make(map[string]string)
	kv.seenOpID = make(map[int64]int)
	kv.outstanding = make(map[int64]bool)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
