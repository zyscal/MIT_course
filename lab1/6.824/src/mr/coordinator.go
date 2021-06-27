package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"

type Coordinator struct {
	// Your definitions here.
	fill_list []string
	check_fill_map []bool
	bucket_pos int
	worker_id_map_list []int
	nReduce int
	reduceFillnum int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) GetreduceFill(arg *ReduceArgs, reply *ReduceRep) error{
	fmt.Println("enter into Getreduce")
	if c.reduceFillnum < c.nReduce {
		reply.Y = c.reduceFillnum
		c.reduceFillnum += 1
	}else{
		reply.Y = -1
	}
	return nil
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	fmt.Println("enter into example")
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetFill(arg *FillNameArgs, rep *FillNameRep) error{
	fmt.Println("enter into GetFill the worker id is : ", arg.Worker_map_id)
	i := 0
	for ; i < len(c.worker_id_map_list); i++{
		if c.worker_id_map_list[i] == arg.Worker_map_id {
			rep.Worker_id = i
			break
		}
	}
	if i == len(c.worker_id_map_list){
		fmt.Println("now worker_id_map size : ", len(c.worker_id_map_list))
		rep.Worker_id = i
		c.worker_id_map_list = append(c.worker_id_map_list, arg.Worker_map_id)
	}

	j := 0
	for ; j < len(c.check_fill_map); j++{
		if c.check_fill_map[j] == false{
			c.check_fill_map[j] = true
			rep.Finish = false
			rep.Name = c.fill_list[j]
			rep.Bucket_id = c.bucket_pos
			c.bucket_pos++
			c.bucket_pos = c.bucket_pos % c.nReduce
			break;
		}
	}
	if j == len(c.check_fill_map){
		rep.Finish = true
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.bucket_pos = 0
	c.nReduce = nReduce
	c.reduceFillnum = 0
	for i := 0; i < len(files); i++ {
		c.fill_list = append(c.fill_list, files[i])
		c.check_fill_map = append(c.check_fill_map, false)
	}
	// Your code here.
	fmt.Println(c.fill_list)
	fmt.Println(c.check_fill_map)
	c.server()
	return &c
}
