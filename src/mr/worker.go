package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "sort"
import "strings"
import "sync"
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
var worker_lock sync.Mutex

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func loadDataFromIntermediateFile(file_name string) []KeyValue{
	input_file, _ := os.Open(file_name)
	kva := []KeyValue{}
	Key := ""
	Value := ""
	for {

		fmt.Fscanf(input_file, "%s %s\n", &Key, &Value)
		if Key == "END" && Value == "END"{
			break
		}
		item := KeyValue{
				Key:Key, 
				Value:Value,
			}
		kva = append(kva, item)
	}
	return kva
}

func getReduceFileName(task_file string) string{

	oname := "mr-out-"
	res := strings.Split(task_file, "-")
	index := res[len(res) - 1]
	return oname + index
}
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	
	my_id := "NONE"
	pong_message := &PongMessage{
		Worker_id: "NONE",
		Reducer_count:0,
		Task_type: "NONE",
		Task_files: []string{},
		Exit: false,

	}
	var reducer int
	ping_message := PingMessage{
		Worker_id : "NONE",
		Task_type : "NONE",
		Task_files : []string{},
		Result_files : []string{},
	}

	for {
		worker_lock.Lock()
		// Sync with master
		Sync(ping_message, pong_message)		
		
		//check if i need to shut down
		if pong_message.Exit{
			worker_lock.Unlock()
			os.Exit(0)
		}
		
		// extract task information 
		my_id = pong_message.Worker_id
		reducer = pong_message.Reducer_count
		task_files := pong_message.Task_files
		task_type := pong_message.Task_type
		
		// master didnt assign task for me 
		if task_type == "NONE"{
			 ping_message = PingMessage{
				Worker_id : my_id,
				Task_type : "NONE",
				Task_files : []string{},
				Result_files : []string{},
			
			}

		}
		// begin to process map task 
		if task_type == "MAP"{
			//log.Println(my_id, "------: receive map task from master")	
			intermediate := []KeyValue{}
			filename := task_files[0]
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			
			kva := mapf(filename, string(content))
			intermediate = append(intermediate, kva...)
			
			mapper_id := RandomString(6)
			file_names := []string{}
			file_pointers := []*os.File{}
			index := 0	
			// create all file handlers and intermediate file names 
			for index =0; index< reducer; index++{
				file_name:= "../" + mapper_id + "-" + fmt.Sprintf("%d", index)
				ofile, _ := os.Create(file_name)
			        file_pointers = append(file_pointers, ofile)	
				file_names = append(file_names, file_name)
			}

			
			// write all data to intermediate files
			for index =0; index < len(intermediate); index++{
				reducer_index := ihash(intermediate[index].Key) % reducer
				fmt.Fprintf(file_pointers[reducer_index], "%s %s\n", intermediate[index].Key, intermediate[index].Value)
				
			}
			
			// write end characters and close all files
			for index = 0; index < len(file_pointers); index++{
				fmt.Fprintf(file_pointers[index], "%s %s", "END", "END")
				file_pointers[index].Close()
			}
			
			//fullfill ping_message 
			
			ping_message = PingMessage{
				Worker_id : my_id,
				Task_type : "MAP",
				Task_files : task_files,
				Result_files : file_names,
			
			}
		}
		
		if  task_type == "REDUCE"{

			intermediate := []KeyValue{}
			oname  := getReduceFileName(task_files[0])
			//log.Println(my_id, "--------------- :check reduce task files ", task_files)
			// read all data into intermediate
			index := 0
			for index = 0; index < len(task_files); index++{
				kva := loadDataFromIntermediateFile(task_files[index])
			        intermediate = append(intermediate, kva...)
			
			}
			//log.Println(my_id, "-------------: load all reduce files successed")
			// sort all keys 
			
			sort.Sort(ByKey(intermediate))
			ofile, _ := os.Create(oname)

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				// group all keys 
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			ofile.Close()

			// fullfill pong message 
			ping_message = PingMessage{
				Worker_id : my_id,
				Task_type : "REDUCE",
				Task_files : task_files,
				Result_files : []string{oname},
			}



		}
		worker_lock.Unlock()


	}

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func Sync(ping_message PingMessage, pong_message *PongMessage) {


	// send the RPC request, wait for the reply.
	call("Master.Sync", &ping_message, pong_message)

}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
