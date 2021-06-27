package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "math/rand"
import "math"
import "time"
import "strconv"
import "os"
import "encoding/json"
import "sort"


import "io/ioutil"
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type This_Worker struct {
	Id int
}
type ByKey []KeyValue
func (a ByKey)Len() int {return len(a)}
func (a ByKey)Less(i,j int) bool  {return a[i].Key < a[j].Key}
func (a ByKey)Swap (i,j int)  {a[i],a[j] = a[j], a[i]}


//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
	var this_worker This_Worker
	this_worker.Id = 0


	for{
		time.Sleep(time.Duration(2)*time.Second)
		reply := CallgetFillname(&this_worker)
		fmt.Println("after call : " , reply)
		if reply.Finish{
			break
		}
		filename := reply.Name
		file, _ := os.Open(filename)
		content, _ := ioutil.ReadAll(file)
		file.Close()

		kva := mapf(filename, string(content))
		sort.Sort(ByKey(kva))
		tem_file := "mr-" + strconv.Itoa(reply.Worker_id) + "-" + strconv.Itoa(reply.Bucket_id) + ".json"
		filePtr, err := os.Create(tem_file)
		encoder := json.NewEncoder(filePtr)
		err = encoder.Encode(&kva)
		if err != nil {
			fmt.Println("Encoder failed", err.Error())

		} else {
			fmt.Println("Encoder success")
		}
		filePtr.Close()
	}
	//begin reduce
	for{
		time.Sleep(time.Duration(2) * time.Second)
		reply := CallgetReduce()
		fmt.Println("the reply : ", reply)
		if reply.Y == -1{
			break
		}

	}


}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.

	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func CallgetFillname(worker *This_Worker) FillNameRep{
	args := FillNameArgs{}
	reply := FillNameRep{}
	if worker.Id == 0 {
		rand.Seed(time.Now().UnixNano())
		args.Worker_map_id = rand.Intn(math.MaxUint32)
		worker.Id = args.Worker_map_id
	}else{
		args.Worker_map_id = worker.Id
	}
	fmt.Println(args)
	call("Coordinator.GetFill", &args, &reply)
	return reply
}



func CallgetReduce() ReduceRep {
	args := ReduceArgs{}
	reply := ReduceRep{}
	call("Coordinator.GetreduceFill", &args, &reply)
	return reply
}



//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
