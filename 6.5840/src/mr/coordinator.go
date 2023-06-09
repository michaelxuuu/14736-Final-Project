package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Output file names for trace-based test cases
var LogFileNames = []string{"log_work_conserving", "log_rejoin", "log_locality", "log_task_failure"}

// Directory for intermediate values
const outDir = "tmp"

// Task status
const (
	PENDING = iota // not assigned to a worker yet
	RUNNING
	DONE
)

// Task Types
const (
	MAP_TASK = iota
	REDUCE_TASK
	NULL_TASK // pseudo task: do nothing on receiving this task
	EXIT_TASK // pseudo task: exit on receiving this task
)

// Task-related informations
type Task struct {
	kind   int
	state  int
	file   string // file path
	worker int    // worker to which this task's assigned to
}

// Internal states for coordinator
type Coordinator struct {
	mapTasks    []Task // a list of map tasks (persistent - finished tasks not removed)
	reduceTasks []Task // a list of reudce tasks (persistent - finished tasks not removed)
	mapCount    int    // number of schedulable map tasks
	reduceCount int    // number of schedulable reduce tasks has not yet completed
	mu          sync.Mutex
	failCount   int
	workers     []int
}

// Called once by the worker during their initialization phase
// to get the total number of reduce tasks (this is an argument
// passed to the master during creation by the main routine)
func (c *Coordinator) GetReduceCount(args *GetReduceCountArgs, reply *GetReduceCountReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workers = append(c.workers, args.WorkerId)
	reply.ReduceCount = len(c.reduceTasks)
	return nil
}

// Locality-aware scheduling function
func (c *Coordinator) pick(tasks []Task, worker int) (*Task, int) {
	var worker_location string
	for loc, w := range c.workers {
		if worker == w {
			worker_location = strconv.Itoa(loc)
		}
	}

	observed_task_locations := make([]string, 0)
	random_available_task := -1
	random_available_task_loc := ""
	for i := 0; i < len(tasks); i++ {
		file_location := "N/A"
		if len(tasks[i].file) > len("pg-") {
			pos := strings.Index(tasks[i].file, "pg-") + len("pg-")
			file_location = string(tasks[i].file[pos])
		}

		// If locality is achievable, just return
		if tasks[i].state == PENDING {
			random_available_task = i
			random_available_task_loc = file_location

			observed_task_locations = append(observed_task_locations, file_location)

			// Reduce task cannot exploit locality, so any worker is fine
			if file_location == worker_location || tasks[i].kind == REDUCE_TASK {
				if os.Getenv("TEST_LOC") == "1" && tasks[i].kind == MAP_TASK {
					f, _ := os.OpenFile("./../"+LogFileNames[2], os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
					fmt.Fprintln(f, "Observed pending task locations: ", observed_task_locations)
					fmt.Fprintln(f, "Scheduling decision: worker location - ", worker_location, "; data location - ", file_location)
					f.Close()
				}

				tasks[i].state = RUNNING
				tasks[i].worker = worker
				return &tasks[i], i
			}
		}
	}

	// Otherwise, select any task
	if random_available_task >= 0 {
		if os.Getenv("TEST_LOC") == "1" && tasks[random_available_task].kind == MAP_TASK {
			f, _ := os.OpenFile("./../"+LogFileNames[2], os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
			fmt.Fprintln(f, "observed task locations: ", observed_task_locations)
			fmt.Fprintln(f, "schedulign decision: worker location - ", worker_location, "; data location - ", random_available_task_loc)
			f.Close()
		}

		tasks[random_available_task].state = RUNNING
		tasks[random_available_task].worker = worker
		return &tasks[random_available_task], random_available_task
	}

	return &Task{kind: NULL_TASK}, -1
}

// Check task status 10s after it's scheduled. If the task is still running,
// consider it as failed and reschedule.
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
		c.failCount++
		if os.Getenv("TEST_TASK_FAIL") == "1" {
			f, _ := os.OpenFile("./../"+LogFileNames[3], os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
			fmt.Fprintln(f, "task failure count", c.failCount)
			f.Close()
		}
	}

	if c.failCount == 3 && os.Getenv("TEST_TASK_FAIL") == "1" {
		f, _ := os.OpenFile("./../"+LogFileNames[3], os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
		fmt.Fprintln(f, "too many failures, kill the job")
		c.mapCount = 0
		c.reduceCount = 0
		f.Close()
	}
}

// RPC call for workers to pull tasks.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var task *Task
	var idx int

	if c.mapCount != 0 {
		task, idx = c.pick(c.mapTasks, args.WorkerId)
	} else if c.reduceCount != 0 {
		task, idx = c.pick(c.reduceTasks, args.WorkerId)
	} else {
		task = &Task{kind: EXIT_TASK}
		idx = -1
		return nil
	}

	reply.TaskKind = task.kind
	reply.TaskId = idx
	reply.File = task.file

	if task.kind != NULL_TASK {
		pCount := 0
		for i := 0; i < len(c.mapTasks); i++ {
			if c.mapTasks[i].state == PENDING {
				pCount++
			}
		}
		for i := 0; i < len(c.reduceTasks); i++ {
			if c.reduceTasks[i].state == PENDING {
				pCount++
			}
		}
		if os.Getenv("TEST_WORK_RESERVING") == "1" {
			f, _ := os.OpenFile("./../"+LogFileNames[0], os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
			fmt.Fprintln(f, "GetTask: pending count:", pCount)
			f.Close()
		}
	}

	go c.waitForReply(task)

	return nil
}

// RPC call for workers to notify the coordinator upon task completion.
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
	c.failCount = 0

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
