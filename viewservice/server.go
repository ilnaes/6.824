package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

    currentView View
    outstanding bool // is there an outstanding ping to primary
    backup      string

    primaryLast  time.Time
    backupLast   time.Time
	// Your declarations here.
}

func (vs *ViewServer) updateView(primary string, backup string) {
    var cv *View
    cv = &vs.currentView

    if cv.Backup != "" && backup == "" {
        vs.backup = ""
    }

    cv.Primary = primary
    cv.Backup = backup
    cv.Viewnum += 1

    fmt.Printf("NEW   ")
    cv.Print()

    vs.outstanding = true
}

func (cv *View) Print() {
    fmt.Printf("View %d: P - %s      B - %s\n", cv.Viewnum, cv.Primary, cv.Backup)
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
    vs.mu.Lock()
    defer vs.mu.Unlock()
//    fmt.Printf("ping: %d %s\n", args.Viewnum, args.Me)

    cv := vs.currentView

    if vs.currentView.Viewnum == 0 {
        vs.updateView(args.Me, "")
    } else {
        if args.Me == vs.currentView.Primary {
            // primary acked new view
            if vs.outstanding {
                if args.Viewnum == cv.Viewnum {
                    if vs.currentView.Viewnum == args.Viewnum {
                //        fmt.Printf("ACKED ")
                //        vs.Print()
                        vs.outstanding = false

                        if vs.currentView.Backup == "" && vs.backup != "" {
                            // implement backup if any
                 //           fmt.Printf("IMPLEMENT BACKUP ")
                            vs.updateView(cv.Primary, vs.backup)
                            vs.backup = ""
                        }
                    }
                }
            } else if args.Viewnum != cv.Viewnum  {
                // primary has died
           //     fmt.Printf("PRIMARY MISMATCH ")
                vs.updateView(cv.Backup, "")
                vs.primaryLast = vs.backupLast
            }
        } else {
            // no backups at all
            if cv.Backup == "" && vs.backup == "" {
                // primary hasn't acked yet
                if vs.outstanding {
                    vs.backup = args.Me
                } else {
                    vs.updateView(cv.Primary, args.Me)
                }
            }
        }
    }

    if args.Me == cv.Primary  {
        vs.primaryLast = time.Now()
    }

    if args.Me == cv.Backup {
        vs.backupLast = time.Now()
    }

    reply.View = vs.currentView
    //fmt.Printf("PING REPLY!: %d %s %s\n", cv.Viewnum, cv.Primary, cv.Backup)
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

//    cv := vs.currentView
//    fmt.Printf("REPLY!: %d %s %s\n", cv.Viewnum, cv.Primary, cv.Backup)
    reply.View = vs.currentView

	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
    vs.mu.Lock()
    defer vs.mu.Unlock()

    //vs.currentView.Print()
    cv := vs.currentView

    if vs.currentView.Viewnum == 0 {
        return
    }

    // timeouts only allowed if outstanding
    if !vs.outstanding {
        // backup timed out
        if time.Since(vs.backupLast).Seconds() > 0.1 * DeadPings && cv.Backup != "" {
    //        fmt.Printf("BACKUP %s DIED\n", cv.Backup)
            vs.updateView(cv.Primary, "")
            return
        }

        // primary timed out
        if time.Since(vs.primaryLast).Seconds() > 0.1 * DeadPings {
     //       fmt.Printf("PRIMARY %s DIED\n", cv.Primary)
     //       fmt.Println("FAILED!!!!")
            vs.updateView(cv.Backup, "")
            return
        }
    }
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
    vs.currentView = View{0, "", ""}
    vs.outstanding = false
    vs.backup = ""
    vs.primaryLast = time.Now()
    vs.backupLast = time.Now()

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
