package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type SendMessage struct {
	TaskID int
	TaskType int 	//1->map  2->reduce
	TaskCompleteStatus int //1->completed 0->failed
}

type ReplyMessage struct {
	TaskID int
	TaskType int	//0->wait  1->map  2->reduce 3->allComplete
	TaskFile string
	NReduce int
	NMap int
}





// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
