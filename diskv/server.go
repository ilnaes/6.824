package diskv

import "bytes"
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
import "encoding/base32"
import "math/rand"
import "shardmaster"
import "io/ioutil"
import "strconv"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

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

type DisKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos
	dir        string // each replica has its own data directory

	gid int64 // my replica group ID

	// Your definitions here.
	Seq         int
	Current     shardmaster.Config
	Data        map[string]string
	SeenOpID    map[int64]int
	HasShard    [shardmaster.NShards]bool
	Outstanding map[int64]bool
	recovery    bool
}

//
// these are handy functions that might be useful
// for reading and writing key/value files, and
// for reading and writing entire shards.
// puts the key files for each shard in a separate
// directory.
//

func (kv *DisKV) shardDir(shard int) string {
	d := kv.dir + "/shard-" + strconv.Itoa(shard) + "/"
	// create directory if needed.
	_, err := os.Stat(d)
	if err != nil {
		if err := os.Mkdir(d, 0777); err != nil {
			log.Fatalf("Mkdir(%v): %v", d, err)
		}
	}
	return d
}

// cannot use keys in file names directly, since
// they might contain troublesome characters like /.
// base32-encode the key to get a file name.
// base32 rather than base64 b/c Mac has case-insensitive
// file names.
func (kv *DisKV) encodeKey(key string) string {
	return base32.StdEncoding.EncodeToString([]byte(key))
}

func (kv *DisKV) decodeKey(filename string) (string, error) {
	key, err := base32.StdEncoding.DecodeString(filename)
	return string(key), err
}

// read the content of a key's file.
func (kv *DisKV) fileGet(shard int, key string) (string, error) {
	fullname := kv.shardDir(shard) + "/key-" + kv.encodeKey(key)
	content, err := ioutil.ReadFile(fullname)
	return string(content), err
}

// replace the content of a key's file.
// uses rename() to make the replacement atomic with
// respect to crashes.
func (kv *DisKV) filePut(shard int, key string, content string) error {
	fullname := kv.shardDir(shard) + "/key-" + kv.encodeKey(key)
	tempname := kv.shardDir(shard) + "/temp-" + kv.encodeKey(key)
	if err := ioutil.WriteFile(tempname, []byte(content), 0666); err != nil {
		return err
	}
	if err := os.Rename(tempname, fullname); err != nil {
		return err
	}
	return nil
}

// return content of every key file in a given shard.
func (kv *DisKV) fileReadShard(shard int) map[string]string {
	m := map[string]string{}
	d := kv.shardDir(shard)
	files, err := ioutil.ReadDir(d)
	if err != nil {
		log.Fatalf("fileReadShard could not read %v: %v", d, err)
	}
	for _, fi := range files {
		n1 := fi.Name()
		if n1[0:4] == "key-" {
			key, err := kv.decodeKey(n1[4:])
			if err != nil {
				log.Fatalf("fileReadShard bad file name %v: %v", n1, err)
			}
			content, err := kv.fileGet(shard, key)
			if err != nil {
				log.Fatalf("fileReadShard fileGet failed for %v: %v", key, err)
			}
			m[key] = content
		}
	}
	return m
}

// replace an entire shard directory.
func (kv *DisKV) fileReplaceShard(shard int, m map[string]string) {
	d := kv.shardDir(shard)
	os.RemoveAll(d) // remove all existing files from shard.
	for k, v := range m {
		kv.filePut(shard, k, v)
	}
}

func (kv *DisKV) log() {
	// pxname := "/Users/seanli/tmp/paxos"
	kvname := kv.dir + "/kv"
	file, err := os.Create(kvname + "-tmp")
	if err != nil {
		fmt.Println("Couldn't open file!")
		return
	}
	encoder := gob.NewEncoder(file)
	encoder.Encode(kv)
	file.Close()

	os.Rename(kvname+"-tmp", kvname)
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

func (kv *DisKV) getOp(seq int) Op {
	to := 10 * time.Millisecond
	for {
		status, val := kv.px.Status(seq)
		if status == paxos.Decided {
			return val.(Op)
		}

		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (kv *DisKV) handleReq(id int64, shard int, op Op) Err {
	for !kv.isdead() {
		// are we assigned shard?
		if kv.Current.Shards[shard] == kv.gid {

			// do we have shard?
			if kv.HasShard[shard] {

				// have we seen opID before?
				if kv.SeenOpID[id] == 0 {

					kv.Seq++
					seq := kv.Seq

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
				return ErrTryAgain
			}
		} else {
			return ErrWrongGroup
		}
	}
	return "Dead"
}

func (kv *DisKV) process(op Op, seq int, server int64) {
	if kv.me == 0 && Debug != 0 {
		fmt.Printf("%d PROCESSING %d  ", kv.me, seq)
		fmt.Println(op)
	}

	if op.Type == "Reconfig" {
		kv.doReconfig(op)
		kv.log()
		kv.px.Done(seq)
		return
	}
	if op.Type == "Update" {
		kv.doUpdate(op)
		kv.log()
		kv.px.Done(seq)
		return
	}
	if op.Type == "Recover" || op.Type == "Heartbeat" {
		kv.log()
		kv.px.Done(seq)
		return
	}

	if op.Type == "Append" {
		kv.Data[op.Key] += op.Value
	} else if op.Type == "Put" {
		kv.Data[op.Key] = op.Value
	}

	kv.SeenOpID[op.OpID] = seq
	kv.px.Done(seq)

	// did we get a request from op's server?
	if op.Ck == server {
		delete(kv.SeenOpID, op.LastID)
	}

	kv.log()
}

func (kv *DisKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	// fmt.Printf("GET %d %t --- %d\n", kv.me, kv.recovery, kv.Seq+1)
	if !kv.recovery {
		kv.mu.Lock()
		defer kv.mu.Unlock()

		op := Op{OpID: args.OpID, Type: "Get", Key: args.Key, Ck: args.Ck, LastID: args.LastID}
		err := kv.handleReq(args.OpID, key2shard(args.Key), op)

		if err == OK {
			val, ok := kv.Data[args.Key]
			if !ok {
				reply.Err = ErrNoKey
			} else {
				reply.Err = OK
				reply.Value = val
			}
		} else {
			reply.Err = err
		}
	}
	return nil
}

// RPC handler for client Put and Append requests
func (kv *DisKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	// fmt.Printf("PUTAPPEND %d %t --- %d\n", kv.me, kv.recovery, kv.Seq+1)
	if !kv.recovery {
		kv.mu.Lock()
		defer kv.mu.Unlock()

		var op Op
		if args.Op == "Put" {
			op = Op{OpID: args.OpID, Type: "Put", Key: args.Key, Value: args.Value,
				Ck: args.Ck, LastID: args.LastID}
		} else {
			op = Op{OpID: args.OpID, Type: "Append", Key: args.Key, Value: args.Value,
				Ck: args.Ck, LastID: args.LastID}
		}
		reply.Err = kv.handleReq(args.OpID, key2shard(args.Key), op)
	}

	return nil
}

func (kv *DisKV) doUpdate(op Op) {
	if kv.Current.Num == op.View {
		if op.Update != nil {
			for k, v := range op.Update {
				kv.Data[k] = v
			}
		}
		for _, i := range op.Shards {
			kv.HasShard[i] = true
		}
		for k, v := range op.Clients {
			kv.SeenOpID[k] = v
		}
		delete(kv.Outstanding, op.GID)
	}
}

func (kv *DisKV) Update(args *UpdateArgs, reply *UpdateReply) error {
	// send update to self
	kv.mu.Lock()
	defer kv.mu.Unlock()

	op := Op{Type: "Update", Update: args.Data, View: args.View, GID: args.From,
		Shards: args.Shards, Clients: args.Clients}
	for !kv.isdead() {
		if op.View == kv.Current.Num {
			if kv.Outstanding[args.From] == false {
				reply.Err = OK
				return nil
			}
			kv.Seq++
			seq := kv.Seq

			kv.px.Start(seq, op)
			xop := kv.getOp(seq)
			kv.process(xop, seq, 0)

			if xop.Type == "Update" && xop.View == op.View && xop.GID == op.GID {
				reply.Err = OK
				return nil
			}
		} else if op.View > kv.Current.Num {
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
func (kv *DisKV) sendUpdates(servers []string, msg map[string]string, shards []int,
	clients map[int64]int, view int, gid int64) {
	args := &UpdateArgs{View: view, Data: msg, Shards: shards, From: kv.gid, Clients: clients}

	for {
		for _, srv := range servers {
			var reply UpdateReply
			ok := call(srv, "DisKV.Update", args, &reply)
			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *DisKV) doReconfig(op Op) {
	if kv.Current.Num != op.Config.Num-1 {
		return
	}

	msg := make(map[int64]map[string]string)
	shards := make(map[int64][]int)

	for i := 0; i < shardmaster.NShards; i++ {
		// find shards to send
		if (op.Config.Shards[i] != kv.gid) && (kv.Current.Shards[i] == kv.gid) {
			kv.HasShard[i] = false
			shards[op.Config.Shards[i]] = append(shards[op.Config.Shards[i]], i)
		}

		// set outstanding flag for shards to be received
		if (op.Config.Shards[i] == kv.gid) && (kv.Current.Shards[i] != kv.gid) && op.Config.Num != 1 {
			kv.Outstanding[kv.Current.Shards[i]] = true
		}

		// first group
		if op.Config.Num == 1 && op.Config.Shards[0] == kv.gid {
			kv.HasShard[i] = true
		}

	}

	for k, v := range kv.Data {
		// write messages
		shard := key2shard(k)
		if (op.Config.Shards[key2shard(k)] != kv.gid) && (kv.Current.Shards[key2shard(k)] == kv.gid) {
			if msg[op.Config.Shards[shard]] == nil {
				msg[op.Config.Shards[shard]] = make(map[string]string)
			}
			msg[op.Config.Shards[shard]][k] = v
		}
	}

	kv.Current = op.Config

	for gid, servers := range op.Config.Groups {
		// send updates
		if gid != kv.gid && len(shards[gid]) > 0 {
			go kv.sendUpdates(servers, msg[gid], shards[gid], copyMap(kv.SeenOpID), op.Config.Num, gid)
		}
	}
}

func fateString(f paxos.Fate) string {
	switch f {
	case paxos.Decided:
		return "Decided"
	case paxos.Pending:
		return "Pending"
	case paxos.Forgotten:
		return "Forgotten"
	default:
		return ""
	}
}

func (kv *DisKV) reconfigure(config shardmaster.Config) {
	op := Op{Type: "Reconfig", Config: config}
	for !kv.isdead() {
		// check if up to date before reconfiging
		if len(kv.Outstanding) == 0 {
			kv.Seq++
			seq := kv.Seq

			kv.px.Start(seq, op)
			xop := kv.getOp(seq)
			kv.process(xop, seq, 0)

			if xop.Type == "Reconfig" && xop.Config.Num == config.Num {
				return
			}
		} else {
			// try to catch up
			status, val := kv.px.Status(kv.Seq + 1)
			if status == paxos.Decided {
				kv.Seq++
				kv.process(val.(Op), kv.Seq, 0)
			}
			return
		}
	}
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *DisKV) tick() {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	nconfig := kv.sm.Query(-1)
	if nconfig.Num > kv.Current.Num {
		if nconfig.Num > kv.Current.Num+1 {
			nconfig = kv.sm.Query(kv.Current.Num + 1)
		}
		kv.reconfigure(nconfig)
	} else {
		op := Op{Type: "Heartbeat", View: kv.me, Ck: rand.Int63()}
		kv.Seq++
		seq := kv.Seq

		kv.px.Start(seq, op)
		xop := kv.getOp(seq)
		kv.process(xop, seq, 0)
	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *DisKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *DisKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *DisKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *DisKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

func (kv *DisKV) Copy(args *RecoverArgs, reply *RecoverReply) error {
	kv.mu.Lock()
	kv.px.Lock()
	var buf1, buf2 bytes.Buffer
	encoder := gob.NewEncoder(&buf1)
	encoder.Encode(kv)
	reply.Kv = buf1.String()

	encoder = gob.NewEncoder(&buf2)
	encoder.Encode(kv.px)
	reply.Px = buf2.String()

	reply.Err = OK
	kv.px.Unlock()
	kv.mu.Unlock()
	return nil
}

func (kv *DisKV) Recover(disk bool) {
	kv.mu.Lock()
	kv.recovery = !disk

	var k DisKV
	if disk {
		// recover from disk
		file, err := os.Open(kv.dir + "/kv")
		if err == nil {
			decoder := gob.NewDecoder(file)
			decoder.Decode(&k)
		}
		file.Close()
		kv.px.Recover(disk, "")
		fmt.Printf("RECOVERING SERVER %d ---- HAVE DISK\n", kv.me)
	} else {
		// get recovery data from someone else
		config := kv.sm.Query(-1)
		args := &RecoverArgs{}
		var reply RecoverReply
		done := false

		for !done {
			for i, srv := range config.Groups[kv.gid] {
				if i != kv.me {
					// send copy requests
					ok := call(srv, "DisKV.Copy", args, &reply)
					if ok && reply.Err == OK {
						done = true

						fmt.Printf("RECOVERING SERVER %d FROM %d ---- LOST DISK\n", kv.me, i)

						// get kv data
						decoder := gob.NewDecoder(bytes.NewBufferString(reply.Kv))
						decoder.Decode(&k)

						// set up paxos for recovery
						kv.px.Recover(disk, reply.Px)
						break
					} else if reply.Err == ErrWrongGroup {
						config = kv.sm.Query(-1)
						break
					}
				}
			}
		}
	}

	kv.recovery = !disk

	kv.Seq = k.Seq
	kv.Current = k.Current
	kv.Data = k.Data
	kv.SeenOpID = k.SeenOpID
	kv.HasShard = k.HasShard
	kv.Outstanding = k.Outstanding

	if k.SeenOpID == nil {
		kv.SeenOpID = make(map[int64]int)
	}
	if k.Outstanding == nil {
		kv.Outstanding = make(map[int64]bool)
	}
	if k.Data == nil {
		kv.Data = make(map[string]string)
	}

	if !disk {
		kv.finishRecovery()
	}
	fmt.Printf("RECOVERED SERVER %d %d\n", kv.me, kv.Seq)

	kv.mu.Unlock()
}

func (kv *DisKV) finishRecovery() {
	op := Op{Type: "Recovery", View: kv.me, Ck: rand.Int63()}
	for !kv.isdead() {
		kv.Seq++
		seq := kv.Seq

		kv.px.Start(seq, op)
		xop := kv.getOp(seq)
		kv.process(xop, seq, 0)

		if xop.Type == "Recovery" && xop.Ck == op.Ck && xop.View == op.View {
			kv.recovery = false
			kv.px.FinishRecovery()
			break
		}
	}
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
// dir is the directory name under which this
//   replica should store all its files.
//   each replica is passed a different directory.
// restart is false the very first time this server
//   is started, and true to indicate a re-start
//   after a crash or after a crash with disk loss.
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int, dir string, restart bool) *DisKV {

	kv := new(DisKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.dir = dir

	// Your initialization code here.
	// Don't call Join().

	// fmt.Printf("STARTING SERVER %d!\n", kv.me)

	log.SetOutput(ioutil.Discard)

	gob.Register(Op{})

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)
	kv.px.SetSave(kv.dir + "/paxos")
	kv.px.SetPrint(true)

	if restart {
		_, err := os.Stat(kv.dir + "/kv")
		kv.Recover(!os.IsNotExist(err))
	} else {
		// starting first time
		kv.Data = make(map[string]string)
		kv.SeenOpID = make(map[int64]int)
		kv.Outstanding = make(map[int64]bool)
		var a [shardmaster.NShards]int64
		kv.Current = shardmaster.Config{Num: 0, Shards: a, Groups: nil}
	}

	log.SetOutput(os.Stdout)

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
				fmt.Printf("DisKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	// go func() {
	// 	for !kv.isdead() {
	// 		kv.mu.Lock()
	// 		status, val := kv.px.Status(kv.Seq + 1)
	// 		if Debug != 0 {
	// 			fmt.Printf("%d TICK! --- %d %s\n", kv.me, kv.Seq+1, fateString(status))
	// 		}
	// 		if status == paxos.Decided {
	// 			kv.Seq++
	// 			kv.process(val.(Op), kv.Seq, 0)
	// 		}
	// 		kv.mu.Unlock()
	// 		time.Sleep(250 * time.Millisecond)
	// 	}
	// }()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
