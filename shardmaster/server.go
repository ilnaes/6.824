package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"

import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"
import "sort"
import "reflect"

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num
	seq     int
	current int
}

type Op struct {
	// Your data here.
	Type    string
	GID     int64
	Servers []string
	Shard   int
	Num     int
}

func copyMap(m map[int64][]string) map[int64][]string {
	if m == nil {
		return nil
	}

	newm := make(map[int64][]string)

	for k, v := range m {
		newm[k] = v
	}

	return newm
}

type GIDCount struct {
	GID int64
	Num int
}

type Counts []GIDCount

func (s Counts) Len() int {
	return len(s)
}

func (s Counts) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Counts) Less(i, j int) bool {
	return s[j].Num < s[i].Num
}

// sorts GIDs in decreasing order of num shards
func sortGIDs(GID []int64, shards [NShards]int64) []GIDCount {
	counts := make([]GIDCount, len(GID))
	GIDmap := make(map[int64]int)

	for i, x := range GID {
		counts[i] = GIDCount{x, 0}
		GIDmap[x] = i
	}
	for _, x := range shards {
		counts[GIDmap[x]].Num++
	}

	sort.Sort(Counts(counts))

	return counts
}

func keys(m map[int64][]string) []int64 {
	keys := reflect.ValueOf(m).MapKeys()
	ret := make([]int64, len(keys))

	for i, k := range keys {
		ret[i] = k.Int()
	}
	return ret
}

func (sm *ShardMaster) doMove(args MoveArgs) {
	// group doesn't exists
	if sm.configs[sm.current].Groups[args.GID] == nil {
		return
	}

	// create group
	oldShards := sm.configs[sm.current].Shards
	newm := copyMap(sm.configs[sm.current].Groups)
	sm.current++

	var config Config
	config.Num = sm.current
	config.Groups = newm
	config.Shards = oldShards

	// move shards
	config.Shards[args.Shard] = args.GID

	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) doLeave(args LeaveArgs) {
	// group doesn't exists
	if sm.configs[sm.current].Groups[args.GID] == nil {
		return
	}

	// create group
	newm := copyMap(sm.configs[sm.current].Groups)
	delete(newm, args.GID)
	oldShards := sm.configs[sm.current].Shards

	// transfer shards to some group
	var newg int64
	for _, g := range oldShards {
		if g != args.GID {
			newg = g
			break
		}
	}
	for i, g := range oldShards {
		if g == args.GID {
			oldShards[i] = newg
		}
	}
	sm.current++

	var config Config
	config.Num = sm.current
	config.Groups = newm
	config.Shards = rebalance(keys(newm), oldShards)

	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) doJoin(args JoinArgs) {
	// group already exists
	if sm.configs[sm.current].Groups[args.GID] != nil {
		return
	}

	// create group
	newm := copyMap(sm.configs[sm.current].Groups)
	oldShards := sm.configs[sm.current].Shards
	sm.current++
	newm[args.GID] = args.Servers

	var config Config
	config.Num = sm.current
	config.Groups = newm

	if sm.current > 1 {
		config.Shards = rebalance(keys(newm), oldShards)
	} else {
		for i, _ := range oldShards {
			oldShards[i] = args.GID
		}
		config.Shards = oldShards
	}

	sm.configs = append(sm.configs, config)
}

func min(i, j int) int {
	if i < j {
		return i
	} else {
		return j
	}
}

func rebalance(GIDs []int64, shards [NShards]int64) [NShards]int64 {
	l := len(GIDs)
	base := NShards / l
	overflow := NShards % l
	GIDCounts := sortGIDs(GIDs, shards)

	for i, x := range GIDCounts {
		// i target
		a := base
		if overflow > i {
			a++
		}
		for j := i + 1; j < l; j++ {
			y := GIDCounts[j]
			pad := x.Num - a
			b := base
			if overflow > j {
				b++
			}
			delta := b - y.Num

			// there is something to transfer
			if pad > 0 && delta > 0 {
				move := min(delta, pad)
				y.Num += move
				x.Num -= move

				// move shards
				for i, z := range shards {
					if move <= 0 {
						break
					}
					if z == x.GID {
						shards[i] = y.GID
						move--
					}
				}
			}
		}
	}
	return shards
}

func (sm *ShardMaster) process(xop Op) {
	switch xop.Type {
	case "Join":
		sm.doJoin(JoinArgs{GID: xop.GID, Servers: xop.Servers})
	case "Leave":
		sm.doLeave(LeaveArgs{GID: xop.GID})
	case "Move":
		sm.doMove(MoveArgs{Shard: xop.Shard, GID: xop.GID})
	default:
		return
	}
}

func (sm *ShardMaster) getOp(seq int) Op {
	to := 10 * time.Millisecond
	for {
		status, val := sm.px.Status(seq)
		if status == paxos.Decided {
			return val.(Op)
		}

		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for !sm.isdead() {
		seq := sm.seq + 1
		sm.seq++
		sm.px.Start(seq, Op{Type: "Join", GID: args.GID, Servers: args.Servers})

		xop := sm.getOp(seq)
		sm.process(xop)

		if xop.Type == "Join" && xop.GID == args.GID && len(xop.Servers) == len(args.Servers) {
			br := true
			for i, x := range xop.Servers {
				if x != args.Servers[i] {
					br = false
				}
			}
			if br {
				sm.px.Done(seq)
				break
			}
		}

		/*    if nOp.OpID == args.OpID {
		      val, ok := sm.data[args.Key]
		      if !ok {
		          reply.Err = ErrNoKey
		          reply.Value = ""
		      } else {
		          reply.Err = OK
		          reply.Value = val
		      }
		      return nil
		  } */
	}

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for !sm.isdead() {
		seq := sm.seq + 1
		sm.seq++
		sm.px.Start(seq, Op{Type: "Leave", GID: args.GID})

		xop := sm.getOp(seq)
		sm.process(xop)

		if xop.Type == "Leave" && xop.GID == args.GID {
			sm.px.Done(seq)
			break
		}
	}

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for !sm.isdead() {
		seq := sm.seq + 1
		sm.seq++
		sm.px.Start(seq, Op{Type: "Move", Shard: args.Shard, GID: args.GID})

		xop := sm.getOp(seq)
		sm.process(xop)

		if xop.Type == "Move" && xop.Shard == args.Shard && xop.GID == args.GID {
			sm.px.Done(seq)
			break
		}
	}

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for !sm.isdead() {
		seq := sm.seq + 1
		sm.seq++
		sm.px.Start(seq, Op{Type: "Query", Num: args.Num})

		xop := sm.getOp(seq)
		sm.process(xop)

		if xop.Type == "Query" {
			if args.Num == -1 || args.Num > sm.current {
				//        fmt.Printf("QUERY1: ")
				//        fmt.Println(sm.configs[sm.current])
				reply.Config = sm.configs[sm.current]
			} else {
				//         fmt.Printf("QUERY2: ")
				//         fmt.Println(sm.configs[args.Num])
				reply.Config = sm.configs[args.Num]
			}
			sm.px.Done(seq)
			break
		}
	}
	// Your code here.

	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

// call this to find out if the server is dead.
func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
	sm.seq = 0
	sm.current = 0

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
