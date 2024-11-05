package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	//轮询请求任务
	for {

		args := SendMessage{}
		reply := ReplyMessage{}

		call("Coordinator.RequestTask", &args, &reply)
		switch reply.TaskType {
		case 0:
			time.Sleep(1 * time.Second)
		case 1:
			HandleMapTask(&reply, mapf)
		case 2:
			HandleReduceTask(&reply, reducef)
		case 3:
			os.Exit(1)
		default:
			time.Sleep(1 * time.Second)

		}
	}

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func HandleMapTask(reply *ReplyMessage, mapf func(string, string) []KeyValue) {
	log.Printf("Map worker %v start to handle map task", reply.TaskID)

	

	file, err := os.Open(reply.TaskFile)
	if err != nil {
		log.Fatalf("Map Task %v open the file(%v) error:"+err.Error(), reply.TaskID, reply.TaskFile)
		return
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatal("Map worker read the file error:" + err.Error())
		return
	}

	file.Close()

	kvs := mapf(reply.TaskFile, string(content))
	intermidiate := make([][]KeyValue, reply.NReduce)

	for _, kv := range kvs {
		r := ihash(kv.Key) % reply.NReduce
		intermidiate[r] = append(intermidiate[r], kv)
	}

	for r, kvs := range intermidiate {
		oname := fmt.Sprintf("mr-%v-%v", reply.TaskID, r)
		ofile, err := os.CreateTemp("", oname)
		if err != nil {
			log.Fatalf("Create temp file error:" + err.Error())
			return
		}
		enc := json.NewEncoder(ofile)
		for _, kv := range kvs {
			enc.Encode(kv)
		}
		ofile.Close()
		os.Rename(ofile.Name(), oname)
	}

	
	args := SendMessage{
		TaskID:             reply.TaskID,
		TaskType: 1,
		TaskCompleteStatus: 1,
	}
	
	call("Coordinator.ReportTask", &args, &ReplyMessage{})
}

func HandleReduceTask(reply *ReplyMessage, reducef func(string, []string) string) {
	log.Printf("Reduce worker %v start to handle reduce task", reply.TaskID)
	var intermediate []KeyValue //所有输入文件的kvs

	//该IDworker分到的NMap个文件
	r := reply.TaskID
	Nmap := reply.NMap
	var intermediateFiles []string
	for TaskID := 0; TaskID < Nmap; TaskID++ {
		intermediateFiles = append(intermediateFiles, fmt.Sprintf("mr-%d-%d", TaskID, r))
	}
	for _, fileName := range intermediateFiles {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("Reduce Task %d open file error:"+err.Error(), r)
			return
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	//sort kv by key
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	//write kvs to output file
	log.Printf("Write kvs to output file in reduce worker %v", reply.TaskID)
	oname := fmt.Sprintf("mr-out-%v", reply.TaskID)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("Create output file error:" + err.Error())
		return
	}

	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		//传入k对应的多个v值 得到output
		output := reducef(intermediate[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	os.Rename(ofile.Name(), oname)

	args := SendMessage{
		TaskID:             reply.TaskID,
		TaskType:           2,
		TaskCompleteStatus: 1,
	}

	call("Coordinator.ReportTask", &args, &ReplyMessage{})

}
