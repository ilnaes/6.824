package kvpaxos

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

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	Put    = "Put"
	Append = "Append"
	Get    = "Get"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpID   int64
	Type   string
	Key    string
	Value  string
	Server int64
	LastID int64
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	seenOpID map[int64]int
	//    queue       chan Op
	seq int
	//    workerSeq   int
	data map[string]string
	//    doneChan    map[int](chan Op)
	//    resChan     map[int](chan string)
}

func (kv *KVPaxos) PrintLog() {
	file, _ := os.Create(fmt.Sprintf("%d.log", kv.me))
	defer file.Close()
	file.WriteString(fmt.Sprintf("SEQ NUM %d\n", kv.seq))
	for i := 1; i <= kv.seq; i++ {
		file.WriteString(fmt.Sprintf("%d: "+printOp(kv.getOp(i)), i))
	}
}

func printOp(op Op) string {
	if op.Type == "Get" {
		return fmt.Sprintf("Get %s", op.Key)
	} else {
		return fmt.Sprintf("%s %s into %s", op.Type, op.Value, op.Key)
	}
}

// px = paxos.Make(peers []string, me string)
// px.Start(Seq int, v interface{}) -- start agreement on new instance
// px.Status(Seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(Seq int) -- ok to forget all instances <= Seq
// px.Max() int -- highest instance Seq known, or -1
// px.Min() int -- instances before this Seq have been forgotten

// get OpID of decided entry
func (kv *KVPaxos) getOp(seq int) Op {
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

// records and processes any done op
func (kv *KVPaxos) process(op Op, seq int, server int64) {
	kv.seenOpID[op.OpID] = seq

	if op.Type == "Append" {
		kv.data[op.Key] += op.Value
	} else if op.Type == "Put" {
		kv.data[op.Key] = op.Value
	}

	if op.Server == server {
		lastSeq, ok := kv.seenOpID[op.LastID]

		if !ok {
			return
		}
		kv.px.Done(lastSeq)
		delete(kv.seenOpID, op.LastID)
	}
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for !kv.isdead() {
		if kv.seenOpID[args.OpID] == 0 {
			seq := kv.seq + 1
			kv.seq++
			kv.px.Start(seq, Op{args.OpID, "Get", args.Key, "", args.Ck, args.LastID})

			nOp := kv.getOp(seq)
			kv.process(nOp, seq, args.Ck)

			if nOp.OpID == args.OpID {
				val, ok := kv.data[args.Key]
				if !ok {
					reply.Err = ErrNoKey
					reply.Value = ""
				} else {
					reply.Err = OK
					reply.Value = val
				}
				return nil
			}

			/* result, ok := kv.resChan[seq]

			   if !ok {
			       result = make(chan string)
			       kv.resChan[seq] = result
			   }
			   kv.mu.Unlock()

			   reply.Err = OK
			   reply.Value =<- result */
		} else {
			val, ok := kv.data[args.Key]
			if !ok {
				reply.Err = ErrNoKey
				reply.Value = ""
			} else {
				reply.Err = OK
				reply.Value = val
			}
			return nil
		}
	}

	return nil
	/*
	   val, ok := kv.data[args.Key]
	   kv.mu.Unlock()
	   if !ok {
	       reply.Err = ErrNoKey
	       reply.Value = ""
	   } else {
	       reply.Err = OK
	       reply.Value = val
	   } */
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for !kv.isdead() {
		if kv.seenOpID[args.OpID] == 0 {
			seq := kv.seq + 1
			kv.seq++
			kv.px.Start(seq, Op{args.OpID, args.Op, args.Key, args.Value, args.Ck, args.LastID})

			nOp := kv.getOp(seq)
			kv.process(nOp, seq, args.Ck)

			if nOp.OpID == args.OpID {
				reply.Err = OK
				return nil
			}
		} else {
			reply.Err = OK
			return nil
		}
	}

	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

/*
func (kv *KVPaxos) handleJobs() {
    for {
        // get job
        op := <- kv.queue
        decided := false
        for !decided {
            // start
            seq := kv.px.Max() + 1
            kv.px.Start(seq, op)

            // check status
            to := 10 * time.Millisecond
            for {
                status, entry := kv.px.Status(seq)
                if status == paxos.Decided {
                    if entry.(Op).OpID == op.OpID {
                        decided = true
                    }
                    break
                }
                time.Sleep(to)
                if to < 10 * time.Second {
                    to *= 2
                }
            }
        }
    }
} */

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me
	kv.seq = 0
	kv.seenOpID = make(map[int64]int)
	kv.data = make(map[string]string)

	// Your initialization code here.

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)
	/*    kv.queue = make(chan Op)
	      kv.doneChan = make(map[int](chan Op))
	      kv.resChan = make(map[int](chan string))
	      kv.workerSeq = 1 */

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// launch worker daemon
	/*    go func() {
	           for kv.isdead() == false {
	               // check next op
	               kv.mu.Lock()
	               opChan, ok := kv.doneChan[kv.workerSeq]

	               if !ok {
	                   // leave channel
	                   opChan = make(chan Op, 1)
	                   kv.doneChan[kv.workerSeq] = opChan
	               }
	               kv.mu.Unlock()

	               // receive op
	      //         fmt.Printf("%d --- DAEMON OP WAIT\n", kv.me)

	               op :=<- opChan
	       //        fmt.Printf("%d --- DAEMON %d: %s\n", kv.me, kv.workerSeq, printOp(op))

	               // do get
	               if op.Type == "Get" && op.Server == kv.me {
	        //           fmt.Printf("LOCK\n")
	                   kv.mu.Lock()
	                   result, ok := kv.resChan[kv.workerSeq]

	                   if !ok {
	                       result = make(chan string)
	                       kv.resChan[kv.workerSeq] = result
	                   }
	                   kv.mu.Unlock()
	         //          fmt.Printf("UNLOCK\n")

	                   // send over channel
	                   result <- kv.data[op.Key]
	          //         fmt.Printf("SEND\n")
	               } else {
	                   if op.Type == "Put" {
	                       kv.data[op.Key] = op.Value
	                   } else if op.Type == "Append" {
	                       kv.data[op.Key] += op.Value
	                   }
	               }
	           //    fmt.Printf("DONE DAEMON\n")

	               kv.workerSeq++
	           }
	       }() */

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
