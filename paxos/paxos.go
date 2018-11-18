package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a Sequence of agreed-on Values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(Seq int, v interface{}) -- start agreement on new instance
// px.Status(Seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(Seq int) -- ok to forget all instances <= Seq
// px.Max() int -- highest instance Seq known, or -1
// px.Min() int -- instances before this Seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "bytes"
import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"
import "time"
import "encoding/gob"

const Debug = false

// px.Status() return Values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// result map[int]interface{}
	// Your data here.
	saveDir string
	save    bool

	// Handler func(int)

	Stati     map[int]Fate
	Hi        int
	Lo        int
	Hiprepare map[int]int
	Hiaccept  map[int]int
	Val       map[int]interface{}
	DoneSeqs  []int
	base      int
	recovery  bool
	printing  bool
}

type PrepareArgs struct {
	Seq int
	N   int
}

type AcceptArgs struct {
	Seq int
	N   int
	Val interface{}
}

type DecidedArgs struct {
	Seq int
	Val interface{}
}

type DoneArgs struct {
	Num int
	Me  int
}

type PrepareReply struct {
	Num      int
	High     int
	Val      interface{}
	Accepted bool
}

type AcceptReply struct {
	Num int
}

type DecidedReply struct {
}

type DoneReply struct {
	Num int
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return Value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only Valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			// fmt.Printf("paxos %s Dial() failed: %v\n", name, err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (px *Paxos) SetPrint(b bool) {
	px.printing = b
}

func (px *Paxos) Lock() {
	px.mu.Lock()
}

func (px *Paxos) Unlock() {
	px.mu.Unlock()
}

func (px *Paxos) SetSave(dir string) {
	px.save = true
	px.saveDir = dir
}

// Recover data from log
func (px *Paxos) Recover(disk bool, s string) {
	var p Paxos

	if disk {
		file, err := os.Open(px.saveDir)
		if err == nil {
			decoder := gob.NewDecoder(file)
			decoder.Decode(&p)
		}
		file.Close()
	} else {
		decoder := gob.NewDecoder(bytes.NewBufferString(s))
		decoder.Decode(&p)

		px.base = p.Hi + 1
	}

	px.mu.Lock()
	px.recovery = !disk
	px.Stati = p.Stati
	px.Hiprepare = p.Hiprepare
	px.Hiaccept = p.Hiaccept
	px.DoneSeqs = p.DoneSeqs
	px.Val = p.Val
	px.Hi = p.Hi
	px.Lo = p.Lo
	if px.printing && Debug {
		fmt.Printf("RECOVERING PAXOS!  %d %d\n", px.me, px.Hi)
	}

	px.mu.Unlock()
}

func (px *Paxos) FinishRecovery() {
	px.recovery = false
}

func (px *Paxos) log() {
	if !px.save {
		return
	}

	file, err := os.Create(px.saveDir + "-tmp")
	if err != nil {
		fmt.Println("Couldn't open file!")
		return
	}
	encoder := gob.NewEncoder(file)
	encoder.Encode(px)

	os.Rename(px.saveDir+"-tmp", px.saveDir)
}

func (px *Paxos) Prepare(args PrepareArgs, reply *PrepareReply) error {
	// if px.printing && Debug {
	// 	fmt.Printf("PREPARE %d %d %t\n", px.me, args.Seq, px.recovery)
	// }
	if !px.recovery {
		px.mu.Lock()

		hi, ok := px.Hiprepare[args.Seq]
		if !ok || args.N > hi {
			// if higher prepare then all others
			px.Hiprepare[args.Seq] = args.N

			reply.Accepted = true
			reply.Num = args.N

			k, ok := px.Hiaccept[args.Seq]
			if ok {
				// had accepted before
				reply.High = k
				reply.Val = px.Val[args.Seq]
			} else {
				reply.High = 0
			}
			px.log()
			px.mu.Unlock()
		} else {
			// reply with higher
			px.mu.Unlock()
			reply.Accepted = false
			reply.Num = hi
		}
	}
	return nil
}

func (px *Paxos) Accept(args AcceptArgs, reply *AcceptReply) error {
	// if px.printing {
	// 	fmt.Printf("ACCEPT %d %d %t\n", px.me, args.Seq, px.recovery)
	// }
	if !px.recovery {
		px.mu.Lock()
		hi, ok := px.Hiprepare[args.Seq]
		if !ok || args.N >= hi {
			px.Hiprepare[args.Seq] = args.N
			px.Hiaccept[args.Seq] = args.N
			px.Val[args.Seq] = args.Val

			if px.Hi < args.Seq {
				px.Hi = args.Seq
			}
			px.log()
			px.mu.Unlock()
			reply.Num = args.N
		} else {
			px.mu.Unlock()
			reply.Num = hi
		}
	}

	return nil
}

func (px *Paxos) Decided(args DecidedArgs, reply *DecidedReply) error {
	px.mu.Lock()
	px.Stati[args.Seq] = Decided
	px.Val[args.Seq] = args.Val
	px.log()
	px.mu.Unlock()

	// if px.recovery {
	// 	if px.printing {
	// 		fmt.Printf("RECOVERED PAXOS %d!\n", px.me)
	// 	}
	// 	if args.Seq > px.base {
	// 		px.recovery = false
	// 		px.Handler(args.Seq)
	// 	}
	// }
	return nil
}

func (px *Paxos) isDecided(seq int) bool {
	px.mu.Lock()
	Val := px.Stati[seq]
	px.mu.Unlock()

	return Val == Decided
}

func (px *Paxos) sendPrepare(Seq int, n int, v interface{}) (bool, int, interface{}) {
	// take highest prepare number number
	prepareAccepted := 0
	currentHigh := 0
	highest := true

	// call prepare to all servers
	for i, server := range px.peers {
		// if px.printing && Debug {
		// 	fmt.Printf("SENDING PREPARE %d %d %t\n", px.me, Seq, px.recovery)
		// }
		var reply PrepareReply
		ok := true

		if i == px.me {
			px.Prepare(PrepareArgs{Seq, n}, &reply)
		} else {
			ok = call(server, "Paxos.Prepare", PrepareArgs{Seq, n}, &reply)
		}

		if i == px.me || ok {
			if reply.Accepted {
				// get all accepted prepares
				prepareAccepted++

				// had accepted
				if currentHigh < reply.High {
					v = reply.Val
					currentHigh = reply.High
				}
			} else if reply.Num > 0 {
				// found a higher prepare number
				px.mu.Lock()
				if px.Hiprepare[Seq] < reply.Num {
					px.Hiprepare[Seq] = reply.Num
				}
				px.mu.Unlock()
				return false, 0, v
			}
		}
	}

	return highest, prepareAccepted, v
}

func (px *Paxos) propose(Seq int, v interface{}) {
	px.mu.Lock()
	px.Stati[Seq] = Pending
	px.mu.Unlock()

	if px.Hi < Seq {
		px.Hi = Seq
	}

	// while not decided
	for !px.isDecided(Seq) && !px.isdead() {
		// if px.printing && px.me == 2 {
		// 	fmt.Printf("PROPOSE %d %d %t --- ", px.me, Seq, px.recovery)
		// 	fmt.Println(v)
		// }
		px.mu.Lock()
		n := px.Hiprepare[Seq] + 1
		px.mu.Unlock()
		highest, prepareAccepted, newv := px.sendPrepare(Seq, n, v)

		// prepare not accepted
		if !highest || prepareAccepted <= len(px.peers)/2 {
			time.Sleep(time.Duration(rand.Int63n(20)) * time.Millisecond)
			continue
		} else {
			v = newv
		}

		acceptAccepted := px.sendAccepted(Seq, n, v)

		// accept not accepted
		if acceptAccepted <= len(px.peers)/2 {
			time.Sleep(time.Duration(rand.Int63n(20)) * time.Millisecond)
			continue
		}

		if px.printing && Debug {
			fmt.Printf("%d DECIDED %d --- ", px.me, Seq)
			fmt.Println(v)
		}

		// send decided
		for i, server := range px.peers {
			var reply DecidedReply

			if i == px.me {
				// local call self
				px.Decided(DecidedArgs{Seq, v}, &reply)
			} else {
				// RPC call others
				call(server, "Paxos.Decided", DecidedArgs{Seq, v}, &reply)
			}
		}
	}
}

func (px *Paxos) sendAccepted(Seq int, n int, v interface{}) int {
	acceptAccepted := 0

	// send accept
	for i, server := range px.peers {
		var reply AcceptReply
		ok := true

		if i == px.me {
			// local call self
			px.Accept(AcceptArgs{Seq, n, v}, &reply)
		} else {
			// RPC call others
			//fmt.Printf("ACCEPT %d:  %d to %d\n",Seq,px.me,i)
			ok = call(server, "Paxos.Accept", AcceptArgs{Seq, n, v}, &reply)
		}

		if ok {
			if reply.Num == n {
				// get all accepted accepts
				acceptAccepted++
			}
		}
	}

	return acceptAccepted
}

func fateString(f Fate) string {
	switch f {
	case Decided:
		return "Decided"
	case Pending:
		return "Pending"
	case Forgotten:
		return "Forgotten"
	default:
		return ""
	}
}

//
// the application wants paxos to start agreement on
// instance Seq, with proposed Value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	px.mu.Lock()
	s, ok := px.Stati[seq]
	// fmt.Printf("STILL GOING! %d %#v\n", seq, v)
	if px.printing && px.me == 0 && Debug {
		fmt.Printf("START %d %d --- %s ", px.me, seq, fateString(s))
		fmt.Println(v)
	}

	if px.DoneSeqs[px.me] <= seq && (!ok || s == Pending) {
		px.mu.Unlock()
		go px.propose(seq, v)
	} else {
		px.mu.Unlock()
	}
}

//
// the application wants to know the
// highest instance Sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	return px.Hi
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	return px.Lo
}

func (px *Paxos) updateMin() {
	px.mu.Lock()
	defer px.mu.Unlock()
	newmin := px.DoneSeqs[px.me]

	for i := 0; i < len(px.peers); i++ {
		if newmin > px.DoneSeqs[i] {
			newmin = px.DoneSeqs[i]
		}
	}

	if newmin >= px.Lo {
		px.Lo = newmin + 1

		for key := range px.Val {
			if key <= newmin {
				delete(px.Hiprepare, key)
				delete(px.Hiaccept, key)
				delete(px.Val, key)
				// delete(px.result, key)
				px.Stati[key] = Forgotten
			}
		}
		px.log()
	}
}

func (px *Paxos) ReplyDone(args DoneArgs, reply *DoneReply) error {
	px.mu.Lock()
	px.DoneSeqs[args.Me] = args.Num
	reply.Num = px.DoneSeqs[px.me]
	px.mu.Unlock()
	px.updateMin()

	return nil
}

func (px *Paxos) propDone() {
	for i, server := range px.peers {
		var reply DoneReply

		if i != px.me {
			ok := call(server, "Paxos.ReplyDone", DoneArgs{px.DoneSeqs[px.me], px.me}, &reply)
			if ok {
				px.mu.Lock()
				px.DoneSeqs[i] = reply.Num
				px.mu.Unlock()
			}
		}
	}
	px.updateMin()
}

//
// the application on this machine is done with
// all instances <= Seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// fmt.Printf("DONE %d: %d\n", px.me, seq)
	px.mu.Lock()
	if px.DoneSeqs[px.me] < seq {
		px.DoneSeqs[px.me] = seq
	}
	px.mu.Unlock()
	px.propDone()
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed Value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(Seq int) (Fate, interface{}) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	Val, ok := px.Stati[Seq]

	if !ok {
		return Pending, nil
	}

	if Val == Decided {
		// return Decided, px.result[Seq]
		return Decided, px.Val[Seq]
	}
	return Val, nil
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.Stati = make(map[int]Fate)
	// px.result = make(map[int]interface{})
	px.Hiprepare = make(map[int]int)
	px.Hiaccept = make(map[int]int)
	px.Val = make(map[int]interface{})
	px.Hi = 0
	px.Lo = 0

	for i := 0; i < len(peers); i++ {
		px.DoneSeqs = append(px.DoneSeqs, -1)
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
