package mapreduce

import (
	"fmt"
	"net/rpc"
)
// DoTaskArgs结构体保存参数，用于为worker分配工作。
type DoTaskArgs struct {
	JobName    string   // 工作的名字
	File       string   // 待处理的文件名
	Phase      jobPhase // 工作类型是map还是reduce
	TaskNumber int      // 任务的索引？

	// 全部任务数量，mapper需要这个数字去计算输出的数量, 同时reducer需要知道有多少输入文件需要收集。
	NumOtherPhase int
}
// ShutdownReply是WorkerShutdown的回应,Ntasks表示worker从启动开始已经处理的任务。
type ShutdownReply struct {
	Ntasks int
}

// worker注册到master的时候，传递的参数。
type RegisterArgs struct {
	Worker string
}
// 本地的rpc调用，使用是unix套接字
func call(srv string, rpcname string,
	args interface{},reply interface{}) bool {

	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
