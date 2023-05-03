package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Argument struct for GetReduceCount RPC
type GetReduceCountArgs struct {
	WorkerId int
}

// Reply struct for GetReduceCount RPC
type GetReduceCountReply struct {
	ReduceCount int
}

// Argument struct for GetTask RPC
type GetTaskArgs struct {
	WorkerId int // worker must identify itself to the coordinate using this field
}

// Reply struct for GetTaskReply RPC
type GetTaskReply struct {
	// perform "Type" task on "File" file
	// Idx to send back to the coordinator to help identifying the task on its side
	TaskKind int    // a map, reduce, null, or exit task
	TaskId   int    // just the index of the task in the task list maintained by the coordinator
	File     string // path to the original file (for the map task) or the intermediate file (for the reduece task) in the DFS
}

// Argument struct for SubmitTask RPC
type SubmitTaskArgs struct {
	WorkerId int // who am i
	TaskId   int // use the idx provided by the coordinator in GetTaskReply
	TaskKind int // instruct the coordinator which task list (map tasks/ reduce tasks) to search for the task
}

// Reply struct for SubmitTask RPC
type SubmitTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp for the coordinator
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
