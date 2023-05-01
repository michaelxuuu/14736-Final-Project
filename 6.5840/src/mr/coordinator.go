package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const outDir = "tmp"
const (
	PENDING = iota // not assigned to a worker yet
	RUNNING
	DONE
)

const (
	MAP_TASK = iota
	REDUCE_TASK
	NULL_TASK // pseudo task: do nothing on receiving this task
	EXIT_TASK // pseudo task: exit on receiving this task
)

type Task struct {
	kind   int
	state  int
	file   string // file path
	worker int    // worker to which this task's assigned to
}

type Coordinator struct {
	// Your definitions here.
	mapTasks    []Task // a list of map tasks (persistent - finished tasks not removed)
	reduceTasks []Task // a list of reudce tasks (persistent - finished tasks not removed)
	mapCount    int    // number of schedulable map tasks
	reduceCount int    // number of schedulable reduce tasks has not yet completed
	mu          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// Called once by the worker during their initialization phase
// to get the total number of reduce tasks (this is an argument
// passed to the master during creation by the main routine)
func (c *Coordinator) GetReduceCount(args *GetReduceCountArgs, reply *GetReduceCountReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	reply.ReduceCount = len(c.reduceTasks)
	return nil
}

func pick(tasks []Task, worker int) (*Task, int) {
	for i := 0; i < len(tasks); i++ {
		if tasks[i].state == PENDING {
			tasks[i].state = RUNNING
			tasks[i].worker = worker
			return &tasks[i], i
		}
	}
	return &Task{kind: NULL_TASK}, -1
}

func (c *Coordinator) waitForReply(task *Task) {
	// only wait for map or reduce tasks
	if task.kind != MAP_TASK && task.kind != REDUCE_TASK {
		return
	}

	// wait for 10s
	<-time.After(time.Second * 10)

	c.mu.Lock()
	defer c.mu.Unlock()
	// assume failure if the task is still running after waiting for 10s
	if task.state == RUNNING {
		task.state = PENDING // make it schedulable again
	}
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var task *Task
	var idx int

	if c.mapCount != 0 {
		task, idx = pick(c.mapTasks, args.WorkerId)
	} else if c.reduceCount != 0 {
		task, idx = pick(c.reduceTasks, args.WorkerId)
	} else {
		task = &Task{kind: EXIT_TASK}
		idx = -1
		return nil
	}

	reply.TaskKind = task.kind
	reply.TaskId = idx
	reply.File = task.file

	// if task.kind != NULL_TASK {
	// 	fmt.Println("hand:", task.file, task.kind, c.mapCount, c.reduceCount)
	// }

	go c.waitForReply(task)

	return nil
}

func (c *Coordinator) SubmitTask(args *SubmitTaskArgs, reply *SubmitTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	taskIdx := args.TaskId
	taskKind := args.TaskKind
	worker := args.WorkerId

	if taskIdx == -1 {
		fmt.Println("-1 index returned")
		os.Exit(1)
	}

	var task *Task
	if taskKind == MAP_TASK {
		task = &c.mapTasks[taskIdx]
	} else if taskKind == REDUCE_TASK {
		task = &c.reduceTasks[taskIdx]
	} else {
		fmt.Println("error unknown type")
		os.Exit(1)
	}

	// check for re-assignement due to timeout
	if worker == task.worker && task.state == RUNNING {
		task.state = DONE
		if taskKind == MAP_TASK {
			c.mapCount--
			// fmt.Println("submit: map")
			// for i := 0; i < 8; i++ {
			// 	fmt.Printf("%d,", c.mapTasks[i].state)
			// }
			// fmt.Println(c.mapCount)

		} else {
			c.reduceCount--
			// fmt.Println("submit: red")
			// for i := 0; i < 8; i++ {
			// 	fmt.Printf("%d,", c.mapTasks[i].state)
			// }
			// fmt.Println(c.mapCount)

		}
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
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reduceCount == 0 && c.mapCount == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapCount = len(files)
	c.reduceCount = nReduce

	for i := 0; i < c.mapCount; i++ {
		c.mapTasks = append(c.mapTasks, Task{kind: MAP_TASK, state: PENDING, file: files[i]})
	}

	for i := 0; i < c.reduceCount; i++ {
		c.reduceTasks = append(c.reduceTasks, Task{kind: REDUCE_TASK, state: PENDING})
	}

	c.server()

	outFiles, _ := filepath.Glob("mr-out*")
	for _, file := range outFiles {
		if err := os.Remove(file); err != nil {
			fmt.Println("Error removing", file)
		}
	}
	err := os.RemoveAll(outDir)
	if err != nil {
		fmt.Println("Error removing", outDir)
	}
	err = os.Mkdir(outDir, 0755)
	if err != nil {
		fmt.Println("Error creating", outDir)
	}

	return &c
}
