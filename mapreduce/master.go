package mapreduce

import "container/list"
import "fmt"


type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {

    doneChannel := make(chan bool)
    jobChannel := make(chan *DoJobArgs)
    idleWorkerChannel := make(chan string)

    //find next worker

    getNextWorker := func() string {
        var address string

        select {
            case address = <- mr.registerChannel :
                mr.Workers[address] = &WorkerInfo{address}
            case address = <- idleWorkerChannel :
        }
        return address
    }

    doJob := func(job *DoJobArgs) {
        worker := getNextWorker()
        var reply DoJobReply
        ok := call(worker, "Worker.DoJob", job, &reply)

        if ok {
            doneChannel <- true
            idleWorkerChannel <- worker
        } else {
            jobChannel <- job
        }
    }

    // pair jobs with worker

    go func() {
        for job := range jobChannel {
            go doJob(job)
        }
    }()

    // dispatch map jobs

    go func() {
        for i := 0; i < mr.nMap; i++ {
             job := &DoJobArgs{mr.file, Map, i, mr.nReduce}
             jobChannel <- job
        }
    }()

    // processes done jobs and reassigns workers

    for i := 0; i < mr.nMap; i++ {
        <- doneChannel
    }

    // same for reduce

    go func() {
        for i := 0; i < mr.nReduce; i++ {
             job := &DoJobArgs{mr.file, Reduce, i, mr.nMap}
             jobChannel <- job
        }
    }()

    for i := 0; i < mr.nReduce; i++ {
        <- doneChannel
    }

    close(jobChannel)

	return mr.KillWorkers()
}
