package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// A struct for key-value pair
type KeyValue struct {
	Key   string
	Value string
}

// Level of parallelism for reduce stage. It's an argument for
// creating the coordinator.
var reduceCount int

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// A special variable only used by Rejoin test
var hasDisconnected = 1

// main/mrworker.go calls this function to start a worker.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	n, succ := getReduceCount()
	if succ == false {
		panic("Cannot obtain reduce count. Exit.")
	}
	reduceCount = n

	for {
		reply, succ := requestTask()

		if os.Getenv("TEST_REJOIN") == "1" && hasDisconnected == 1 {
			f, _ := os.OpenFile("./../"+LogFileNames[1], os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
			fmt.Fprintln(f, "worker", getNodeId(), "goes to sleep, will rejoin in 15s")
			time.Sleep(time.Second * 15)
			hasDisconnected = 0
			f.Close()
		}

		if os.Getenv("TEST_REJOIN") == "1" && hasDisconnected == 0 {
			f, _ := os.OpenFile("./../"+LogFileNames[1], os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
			fmt.Fprintln(f, "worker", getNodeId(), "got task of kind:", reply.TaskKind)
			f.Close()
		}

		if os.Getenv("TEST_TASK_FAIL") == "1" {
			f, _ := os.OpenFile("./../"+LogFileNames[3], os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
			fmt.Fprintln(f, "worker", getNodeId(), "failed")
			f.Close()
			return
		}

		time.Sleep(time.Millisecond * 150)

		if succ == false {
			panic("Cannot contact the Coordinator. Exit.")
		}
		if reply.TaskKind == EXIT_TASK {
			fmt.Println("No more work to do. Exit.")
			return
		}

		exit, succ := false, true
		if reply.TaskKind == MAP_TASK {
			execMap(mapf, reply.File, reply.TaskId)
			reportFinishedTask(MAP_TASK, reply.TaskId)
		} else if reply.TaskKind == REDUCE_TASK {
			execReduce(reducef, reply.TaskId)
			reportFinishedTask(REDUCE_TASK, reply.TaskId)
		}

		if exit || !succ {
			return
		}
	}
}

// Call the map function and output the result to intermediate files
func execMap(mapf func(string, string) []KeyValue, filePath string, mapId int) {
	inputSplit, _ := os.Open(filePath)
	defer inputSplit.Close()

	content, _ := ioutil.ReadAll(inputSplit)

	path2Content := mapf(filePath, string(content))

	prefix := fmt.Sprintf("%v/mr-%v", outDir, mapId)

	for i := 0; i < reduceCount; i++ {
		filePath := fmt.Sprintf("%v-%v-%v", prefix, i, getNodeId())
		file, err := os.Create(filePath)
		if err != nil {
			panic(err)
		}
		buf := bufio.NewWriter(file)
		encoder := json.NewEncoder(buf)

		// write intermediate values to corresponding partition
		for _, kv := range path2Content {
			if ihash(kv.Key)%reduceCount == i {
				encoder.Encode(&kv)
			}
		}

		buf.Flush()
		inputSplit.Close()

		// Rename the file to ensure no partial file is read
		finalPath := fmt.Sprintf("%v-%v", prefix, i)
		os.Rename(file.Name(), finalPath)
		file.Close()
	}
}

// Pull the intermediate values, call reduce, and output the result to files
func execReduce(reducef func(string, []string) string, reduceId int) {
	intermediatePath, err := filepath.Glob(fmt.Sprintf("%v/mr-%v-%v", outDir, "*", reduceId))
	if err != nil {
		panic("No intermediate value found.")
	}

	kvMap := make(map[string][]string)

	for _, filePath := range intermediatePath {
		file, _ := os.Open(filePath)

		decoder := json.NewDecoder(file)
		for decoder.More() {
			var kv KeyValue
			err = decoder.Decode(&kv)
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}

	// sort the keys
	keys := make([]string, 0, len(kvMap))
	for k := range kvMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	outputPath := fmt.Sprintf("%v/mr-out-%v-%v", outDir, reduceId, getNodeId())
	file, _ := os.Create(outputPath)

	// Call reduce and write to temp file
	for _, k := range keys {
		v := reducef(k, kvMap[k])
		fmt.Fprintf(file, "%v %v\n", k, v)
	}

	file.Close()
	newPath := fmt.Sprintf("mr-out-%v", reduceId)
	err = os.Rename(outputPath, newPath)
}

// RPC used for requesting task from the coordinator
func requestTask() (*GetTaskReply, bool) {
	args := GetTaskArgs{getNodeId()}
	reply := GetTaskReply{}
	succ := callCoordinator("Coordinator.GetTask", &args, &reply)
	return &reply, succ
}

// RPC used for report finished task to the coordinator
func reportFinishedTask(taskType int, taskId int) (bool, bool) {
	args := SubmitTaskArgs{getNodeId(), taskId, taskType}
	reply := SubmitTaskReply{}
	succ := callCoordinator("Coordinator.SubmitTask", &args, &reply)

	return false, succ
}

// RPC used for getting reduce count from the coordinator
func getReduceCount() (int, bool) {
	args := GetReduceCountArgs{getNodeId()}
	reply := GetReduceCountReply{}
	succ := callCoordinator("Coordinator.GetReduceCount", &args, &reply)

	return reply.ReduceCount, succ
}

// Return a globally unique identifier of the node
func getNodeId() int {
	// Since the mapReduce is designed to run on one machine,
	// we use pid as the unique node identifier.
	return os.Getpid()
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func callCoordinator(rpcname string, args interface{}, reply interface{}) bool {
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
