package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "strings"   
import "strconv"
import "sync"

const IDLE = 0
const IN_PROGRESS = 1 
const COMPLETED= 2
const DEAD = 3

const WORKER_TIMEOUT = 10

var REDUCER int 
var WORKER_ID  int
var lock sync.Mutex

type TaskWorker struct{
    worker_id string
    status int
    tasks []Task
    pong_receive int64
    
} 

type Task struct {
    task_files []string
    task_status int
    worker_id string 
}

type Master struct {
	// Your definitions here.
    	workers []TaskWorker
    	map_tasks []Task
    	reduce_tasks []Task
	can_quit bool

}

func sameTask(task1 []string, task2 []string) bool{
	
	index := 0
	if len(task1) != len(task2){
		return false 
	}
	if len(task1) == 0{
		return false 
	}

	for index = 0; index < len(task1); index ++{
		if task1[index] != task2[index]{
			return false 
		}
	}
	return true 


}

func getReduceIndex(task_file string) int{
	res := strings.Split(task_file, "-")
	
	index := res[1]
	int_index, _ :=strconv.Atoi(index)
	return int_index 

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Sync(ping_message *PingMessage, pong_message *PongMessage) error {
	
	lock.Lock()
	defer lock.Unlock()
	// encounter a new woker, add it to Master worker list 
        worker_id :=  ping_message.Worker_id
	index := 0
	if worker_id == "NONE"{
		worker_id = RandomString(6)
		worker := TaskWorker{
				worker_id: worker_id,
				status:IDLE,
				tasks: []Task{},
				pong_receive: time.Now().Unix(),
				
			}
		pong_message.Worker_id = worker_id
		//worker_id = WORKER_ID
		//WORKER_ID = WORKER_ID + 1
		m.workers = append(m.workers, worker)
		
		//log.Println("meet a new worker ----", worker_id)
	}
	
	// update pong_receive time 

	for index = 0; index < len(m.workers); index++{

		if m.workers[index].worker_id ==  worker_id{
			m.workers[index].pong_receive = time.Now().Unix()
		}
	}

	// update_task_status
	if ping_message.Task_type != "NONE"{
		updated := false
		if ping_message.Task_type == "MAP"{
			for index = 0; index < len(m.map_tasks); index++{
				if sameTask(m.map_tasks[index].task_files, ping_message.Task_files){
					if m.map_tasks[index].task_status == COMPLETED{
						updated = true
						break
					}

					//log.Println("master --------: add new reduce task from ", worker_id, "  update task info ", index)
					m.map_tasks[index].task_status = COMPLETED
					break
				}
			}
			if !updated{
				for index=0; index < len(ping_message.Result_files); index++{
					reducer_index := getReduceIndex(ping_message.Result_files[index])
					m.reduce_tasks[reducer_index].task_files = append(m.reduce_tasks[reducer_index].task_files, ping_message.Result_files[index])
				}

			}
		}
		if ping_message.Task_type == "REDUCE"{
			for index = 0; index < len(m.reduce_tasks); index++{
				if sameTask(m.reduce_tasks[index].task_files, ping_message.Task_files){
					
					//log.Println("master -------: update reduce task information from ", worker_id, " reduce task index :", index)
					m.reduce_tasks[index].task_status = COMPLETED
					break
				}
			}

		}

	}

	// assign map_tasks
	for index = 0; index < len(m.map_tasks); index++{
		if m.map_tasks[index].task_status == IDLE{
			// fullfill pong_message 
			pong_message.Reducer_count = REDUCER
			pong_message.Task_type = "MAP"
			pong_message.Task_files = m.map_tasks[index].task_files
			pong_message.Exit = false
			pong_message.Worker_id = worker_id
			// change task status 
			m.map_tasks[index].task_status = IN_PROGRESS
			m.map_tasks[index].worker_id = worker_id
			//log.Println("assign map task for worker ------", worker_id, " map task index ", index)
			return nil 
		
		}
	}

	// check if all map task is finished 
	for index = 0; index < len(m.map_tasks); index++{
		if m.map_tasks[index].task_status != COMPLETED{
			// fullfill pong_message 
			pong_message.Worker_id = worker_id
			pong_message.Reducer_count = REDUCER
			pong_message.Task_type = "NONE"
			pong_message.Task_files = []string{}
			pong_message.Exit = false

			//log.Println("assign no map task for worker ------", pong_message.Worker_id)
			return nil 
		
		}
	}


	//assign reduce_tasks
	for index =0; index < len(m.reduce_tasks); index++{
		if m.reduce_tasks[index].task_status == IDLE{
			// fullfill pong message 
			pong_message.Worker_id = worker_id
			pong_message.Reducer_count = REDUCER
			pong_message.Task_type = "REDUCE"
			pong_message.Task_files = m.reduce_tasks[index].task_files
			pong_message.Exit = false

			// set task information 
			
			m.reduce_tasks[index].task_status = IN_PROGRESS
			m.reduce_tasks[index].worker_id = ping_message.Worker_id
		
			//log.Println("assign reduce task for worker ------", pong_message.Worker_id, " task index ", index)
			return nil 

		}
	}

	// no task is assigned now check if we can shut down this worker
	for index = 0; index < len(m.reduce_tasks); index++{
		if m.reduce_tasks[index].task_status != COMPLETED{
			pong_message.Worker_id = worker_id
			pong_message.Reducer_count = REDUCER
			pong_message.Task_type = "NONE"
			pong_message.Task_files = []string{}
			pong_message.Exit = false
			
			//log.Println("assign no reduce task for worker ------", worker_id)
			return nil 

		}
	}

	// all task is finished shut down the worker 
	pong_message.Reducer_count = REDUCER
	pong_message.Task_type = "NONE"
	pong_message.Task_files = []string{}
	pong_message.Exit = true
	pong_message.Worker_id = ping_message.Worker_id

	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (m *Master) GarbageClean() {
	
	index := 0
	for index = 0; index < len(m.reduce_tasks); index++{
		file_index := 0
		for file_index = 0;file_index < len(m.reduce_tasks[index].task_files); file_index++{

			os.Remove(m.reduce_tasks[index].task_files[file_index])
		}

	}

}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	lock.Lock()
	
	ret := true
	// check all woker_status and reset tasks related to failed workers
	i:=0 
	for i =0; i< len(m.workers);i++{

		if time.Now().Unix() - m.workers[i].pong_receive > WORKER_TIMEOUT {
			m.workers[i].status = DEAD
			for index:=0; index < len(m.map_tasks); index++{
				if m.map_tasks[index].worker_id == m.workers[i].worker_id && m.map_tasks[index].task_status == IN_PROGRESS{
					m.map_tasks[index].task_status = IDLE
					ret = false
				}
			}
			
			for index:=0; index < len(m.reduce_tasks); index++{
				if m.reduce_tasks[index].worker_id == m.workers[i].worker_id  && m.reduce_tasks[index].task_status == IN_PROGRESS{
					m.reduce_tasks[index].task_status = IDLE
					ret = false
				}
			}
			//log.Println("worker ", m.workers[i].worker_id, " is dead")
			// remove the worker
			m.workers = append(m.workers[:i], m.workers[i+1:]...)

		}
	}
	for i=0; i< len(m.map_tasks); i++{
		if m.map_tasks[i].task_status != COMPLETED{
			ret = false
			//log.Println("master can not be shutdown because map task ", i, "is not finished \n")
		}
		
	}
	for i=0; i< len(m.reduce_tasks); i++{
		if m.reduce_tasks[i].task_status != COMPLETED{
			ret = false 
			
			//log.Println("master can not be shutdown because reduce task ", i, "is not finished \n")
		}
	}
	if ret{
		m.GarbageClean()
	}

	lock.Unlock()

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		workers:[]TaskWorker{},
		map_tasks: []Task{},
		reduce_tasks: []Task{},
		}

	// initial all tasks 
	var task Task
	var index int
	for index =0; index < len(files); index++{
		
		task.task_files = []string{files[index]}
		task.task_status = IDLE
		task.worker_id = "NONE"
		m.map_tasks = append(m.map_tasks, task)
	}
	for index=0; index < nReduce; index++{

		task.task_files = []string{}
		task.task_status = IDLE
		task.worker_id = "NONE"
		m.reduce_tasks = append(m.reduce_tasks, task)
	}
	
	WORKER_ID = 0
	REDUCER = nReduce
	
	m.server()
	return &m
}
