package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"


type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.

    view        *viewservice.View
    data        map[string]string
    req         map[int64]bool
}

func (pb *PBServer) isPrimary() bool {
    return pb.view.Primary == pb.me
}

func (pb *PBServer) isBackup() bool {
    return pb.view.Backup == pb.me
}

func (pb *PBServer) receivable(me string) bool {
    return pb.isPrimary() || (pb.isBackup() && me == pb.view.Primary)
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
    pb.mu.Lock()
    defer pb.mu.Unlock()

    if !pb.isPrimary() && !(pb.isBackup() && args.Me == pb.view.Primary) {
        // update view and try again
        pb.updateView()

        if !pb.isPrimary() && !(pb.isBackup() && args.Me == pb.view.Primary) {
            reply.Err = ErrWrongServer
            return nil
        }
    }

    if pb.isPrimary() && pb.view.Backup != "" {
        var newreply GetReply
        backupUp := true
        newargs := GetArgs{ args.Key, pb.me }
        ok := call(pb.view.Backup, "PBServer.Get", newargs, &newreply)

        for !ok && backupUp {
            time.Sleep(viewservice.PingInterval)
            pb.updateView()
            if pb.view.Backup != "" {
                ok = call(pb.view.Backup, "PBServer.Get", newargs, &newreply)
            } else {
                backupUp = false
            }
        }

        if backupUp && newreply.Err == ErrWrongServer {
            reply.Err = ErrWrongServer
            return nil
        }
    }

    // actually get
    x, ok := pb.data[args.Key]

    if !ok {
        reply.Err = ErrNoKey
    } else {
        reply.Err = OK
        reply.Value = x
    }

	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
    pb.mu.Lock()
    defer pb.mu.Unlock()

    if pb.req[args.Rid] {
        reply.Err = ErrDuplicate
        return nil
    }

    // was not supposed to handle requests
    if !pb.isPrimary() && !(pb.isBackup() && args.Me == pb.view.Primary) {
        // update view and try again
        pb.updateView()

        if !pb.isPrimary() && !(pb.isBackup() && args.Me == pb.view.Primary) {
            reply.Err = ErrWrongServer
            return nil
        }
    }


    // primary with backup
    if pb.isPrimary() && pb.view.Backup != "" {
        // forward to backup
        var newreply PutAppendReply
        backupUp := true
        newargs := PutAppendArgs{ args.Key, args.Value, pb.me, args.Op, args.Rid }

        ok := call(pb.view.Backup, "PBServer.PutAppend", newargs, &newreply)

        // didn't get response from backup
        for !ok && backupUp {
            time.Sleep(viewservice.PingInterval)
            pb.updateView()
            if pb.view.Backup != "" {
                ok = call(pb.view.Backup, "PBServer.PutAppend", newargs, &newreply)
            } else {
                // backup died
                backupUp = false
            }
        }

        if backupUp && newreply.Err == ErrWrongServer {
            reply.Err = ErrWrongServer
            return nil
        }
    }

    // actually put
    if args.Op == PutC {
        pb.data[args.Key] = args.Value
    } else if args.Op == AppendC {
        pb.data[args.Key] = pb.data[args.Key] + args.Value
    }

    pb.req[args.Rid] = true

    reply.Err = OK

	return nil
}

func (pb *PBServer) Copy(args *CopyArgs, reply *CopyReply) error {
    // make sure is backup and sent from primary

    if args.Me == pb.view.Primary {
        pb.mu.Lock()
        defer pb.mu.Unlock()

        for k, v := range args.Data {
            pb.data[k] = v
        }

        for k, v := range args.Req {
            pb.req[k] = v
        }
        reply.Err = OK
    } else {
        reply.Err = ErrWrongServer
    }
    return nil
}

func (pb *PBServer) updateView() {
    wasActive := pb.isPrimary() || pb.isBackup()
    wasPrimary := pb.isPrimary()
    noBackup := pb.view.Backup == ""
    view, ok := pb.vs.Ping(pb.view.Viewnum)

    for ok != nil || view.Viewnum == 0 {
        view, ok = pb.vs.Ping(pb.view.Viewnum)
    }

    pb.view = &view

    // died, wipe data
    if wasActive && (!pb.isPrimary() && !pb.isBackup()) {
        pb.data = make(map[string]string)
        pb.req = make(map[int64]bool)
        return
    }

    // acquired backup, send copy
    if wasPrimary && noBackup && pb.view.Backup != "" {
        var reply CopyReply
        call(pb.view.Backup, "PBServer.Copy", &CopyArgs{ pb.me, pb.req, pb.data }, &reply)
    }
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
    pb.mu.Lock()
    defer pb.mu.Unlock()
    pb.updateView()
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)

	// Your pb.* initializations here.
    pb.data = make(map[string]string)
    pb.req = make(map[int64]bool)
    pb.view = &viewservice.View{0, "", ""}
    pb.updateView()

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
