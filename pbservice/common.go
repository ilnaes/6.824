package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
    ErrDuplicate   = "Duplicate"
)

const (
    PutC        = "Put"
    AppendC     = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
    Me    string
    Op    string
    Rid   int64

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type CopyArgs struct {
    Me string
    Req  map[int64]bool
    Data map[string]string
}

type CopyReply struct {
    Err     Err
}

type GetArgs struct {
	Key string
    Me  string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}


// Your RPC definitions here.
