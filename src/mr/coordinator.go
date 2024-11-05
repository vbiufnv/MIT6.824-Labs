package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	TaskStatus         int //1->分配  0->未分配
	TaskCompleteStatus int //0->未完成 1->完成
	TaskFile           string
	Time               time.Time //判断运行时间
}

type Coordinator struct {
	// Your definitions here.
	NMap              int
	NReduce           int
	MapTasks          []Task
	ReduceTasks       []Task
	MapCompleteNum    int
	ReduceCompleteNum int
	mutex             sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

// map 处理分配请求逻辑
func (c *Coordinator) RequestTask(args *SendMessage, reply *ReplyMessage) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Log the variables in coordinator
	log.Printf("NMap: %v, NReduce: %v, MapCompleteNum: %v, ReduceCompleteNum: %v", c.NMap, c.NReduce, c.MapCompleteNum, c.ReduceCompleteNum)

	//判断任务完成否 未完成分配
	if c.MapCompleteNum != c.NMap {
		for index, task := range c.MapTasks {
			if taskIsAssignable(task) {
				reply.NReduce = c.NReduce
				reply.NMap = c.NMap
				reply.TaskID = index
				reply.TaskFile = task.TaskFile
				reply.TaskType = 1
				c.MapTasks[index].TaskStatus = 1
				c.MapTasks[index].TaskCompleteStatus = 0
				c.MapTasks[index].Time = time.Now()
				return nil
			}
		}

		reply.TaskType = 0
		// Check reply
		log.Printf("Map Task: %v, TaskType: %v, TaskFile: %v", reply.TaskID, reply.TaskType, reply.TaskFile)
		return nil
	}

	log.Printf("Map Complete")

	//reduce
	if c.ReduceCompleteNum != c.NReduce {
		log.Printf("Reduce Start")
		for index, task := range c.ReduceTasks {
			if taskIsAssignable(task) {
				reply.NReduce = c.NReduce
				reply.NMap = c.NMap
				reply.TaskID = index
				reply.TaskType = 2
				c.ReduceTasks[index].TaskStatus = 1
				c.ReduceTasks[index].TaskCompleteStatus = 0
				c.ReduceTasks[index].Time = time.Now()
				return nil
			}

		}
		reply.TaskType = 0
		return nil
	}

	reply.TaskType = 3
	return nil
}

func (c *Coordinator) ReportTask(args *SendMessage, reply *ReplyMessage) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if args.TaskType == 1 && c.MapTasks[args.TaskID].TaskCompleteStatus == 0 {
		c.MapTasks[args.TaskID].TaskCompleteStatus = 1
		c.MapCompleteNum++
		log.Printf("Map task %d complete", args.TaskID)
		return nil
	}
	if args.TaskType == 2 && c.ReduceTasks[args.TaskID].TaskCompleteStatus == 0 {
		c.ReduceTasks[args.TaskID].TaskCompleteStatus = 1
		c.ReduceCompleteNum++
		return nil

	}

	return nil

}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NMap:              len(files),
		NReduce:           nReduce,
		MapTasks:          make([]Task, len(files)),
		ReduceTasks:       make([]Task, nReduce),
		MapCompleteNum:    0,
		ReduceCompleteNum: 0,
		mutex:             sync.Mutex{},
	}

	// Your code here.
	//划分task
	for index := range files {
		c.MapTasks[index] = Task{
			TaskStatus:         0,
			TaskCompleteStatus: 0,
			TaskFile:           files[index],
			Time:               time.Now(),
		}
	}

	for index := range c.ReduceTasks {
		c.ReduceTasks[index] = Task{
			TaskStatus:         0,
			TaskCompleteStatus: 0,
		}
	}

	c.server()

	return &c
}

func taskIsAssignable(task Task) bool {
	

	return task.TaskStatus == 0 || (task.TaskStatus == 1 &&task.TaskCompleteStatus==0&& time.Since(task.Time) > 10*time.Second)
}
