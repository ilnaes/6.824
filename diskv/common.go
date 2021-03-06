package diskv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrTryAgain   = "ErrTryAgain"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpID   int64
	LastID int64
	Ck     int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	OpID   int64
	LastID int64
	Ck     int64
}

type GetReply struct {
	Err   Err
	Value string
}

type UpdateArgs struct {
	Data    map[string]string
	Shards  []int
	View    int
	From    int64
	Clients map[int64]int
}

type UpdateReply struct {
	Err Err
}

type RecoverArgs struct {
}

type RecoverReply struct {
	Px  string
	Kv  string
	Err Err
}
