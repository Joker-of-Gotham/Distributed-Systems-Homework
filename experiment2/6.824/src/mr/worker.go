package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map/Reduce 中间键值对
type KeyValue struct {
	Key   string
	Value string
}

// ihash: 把 key 映射到 [0, nReduce) 的桶
func ihash(key string, nReduce int) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()&0x7fffffff) % nReduce
}

// Worker: 主循环
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	workerID := fmt.Sprintf("worker-%d", os.Getpid())
	log.Printf("Worker started: ID=%s", workerID)

	for {
		// 1. 向 Coordinator 请求任务
		taskReq := TaskRequest{WorkerID: workerID}
		taskResp := TaskResponse{}
		ok := call("Coordinator.AssignTask", &taskReq, &taskResp)
		if !ok {
			// RPC 调用失败，通常是 Coordinator 已经退出
			log.Printf("Worker %s: Coordinator disconnected, exiting", workerID)
			return
		}

		// 2. 根据任务类型处理
		switch taskResp.Type {
		case NoTask:
			// 暂时没有任务可做，休眠后再来
			log.Printf("Worker %s: No task available, sleeping...", workerID)
			time.Sleep(1 * time.Second)

		case MapTask:
			if err := doMapTask(taskResp, mapf, workerID); err != nil {
				log.Printf("Worker %s: MapTask %d failed: %v",
					workerID, taskResp.MapTaskID, err)
				// 出错不直接退出，交给超时机制重新分配
			} else {
				reportTaskDone(workerID, MapTask, taskResp.MapTaskID, nil)
			}

		case ReduceTask:
			if err := doReduceTask(taskResp, reducef, workerID); err != nil {
				log.Printf("Worker %s: ReduceTask %d failed: %v",
					workerID, taskResp.ReduceTaskID, err)
			} else {
				reportTaskDone(workerID, ReduceTask, taskResp.ReduceTaskID, nil)
			}

		case ExitTask:
			// Coordinator 明确告知所有任务已完成，可以正常退出
			log.Printf("Worker %s: received ExitTask, exiting", workerID)
			return

		default:
			log.Printf("Worker %s: unknown task type %d, exiting",
				workerID, taskResp.Type)
			return
		}
	}
}

// 执行 Map 任务：读取输入 → 调用 mapf → 写中间文件
func doMapTask(resp TaskResponse, mapf func(string, string) []KeyValue, workerID string) error {
	mapTaskID := resp.MapTaskID
	inputFile := resp.InputFile
	nReduce := resp.NReduce

	log.Printf("Worker %s: Start MapTask %d (File=%s, nReduce=%d)",
		workerID, mapTaskID, inputFile, nReduce)

	// 1. 读取输入文件
	content, err := ioutil.ReadFile(inputFile)
	if err != nil {
		return fmt.Errorf("read file %s: %v", inputFile, err)
	}

	// 2. 调用用户 Map 函数
	kva := mapf(inputFile, string(content))

	// 3. 按 reduceID 分桶
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		r := ihash(kv.Key, nReduce)
		buckets[r] = append(buckets[r], kv)
	}

	// 4. 为每个 reduce 分区写一个中间文件 mr-X-Y
	for r := 0; r < nReduce; r++ {
		filename := fmt.Sprintf("mr-%d-%d", mapTaskID, r)

		// 临时文件 + 原子重命名，避免看到部分写入的文件
		tmpFile, err := ioutil.TempFile("", fmt.Sprintf("mr-%d-%d-tmp", mapTaskID, r))
		if err != nil {
			return fmt.Errorf("create temp file: %v", err)
		}

		enc := json.NewEncoder(tmpFile)
		for _, kv := range buckets[r] {
			if err := enc.Encode(&kv); err != nil {
				tmpFile.Close()
				os.Remove(tmpFile.Name())
				return fmt.Errorf("encode kv to %s: %v", tmpFile.Name(), err)
			}
		}
		tmpFile.Close()

		if err := os.Rename(tmpFile.Name(), filename); err != nil {
			os.Remove(tmpFile.Name())
			return fmt.Errorf("rename temp file %s to %s: %v", tmpFile.Name(), filename, err)
		}

		log.Printf("Worker %s: MapTask %d: wrote bucket %d to %s",
			workerID, mapTaskID, r, filename)
	}

	log.Printf("Worker %s: Finish MapTask %d", workerID, mapTaskID)
	return nil
}

// 执行 Reduce 任务：读所有中间文件 → 排序合并 → 调用 reducef → 写 mr-out-Y
func doReduceTask(resp TaskResponse, reducef func(string, []string) string, workerID string) error {
	reduceTaskID := resp.ReduceTaskID
	allMapIDs := resp.AllMapTaskIDs

	log.Printf("Worker %s: Start ReduceTask %d (MapIDs=%v)",
		workerID, reduceTaskID, allMapIDs)

	// 1. 收集中间文件中的所有 KeyValue
	var kva []KeyValue
	for _, mapID := range allMapIDs {
		filename := fmt.Sprintf("mr-%d-%d", mapID, reduceTaskID)
		file, err := os.Open(filename)
		if err != nil {
			// 如果某个中间文件不存在，可能是对应 Map 任务从未执行过；
			// crash 测试中可能出现，这里记录日志后继续。
			log.Printf("Worker %s: open %s failed: %v", workerID, filename, err)
			continue
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				// EOF 或其他错误，直接跳出
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	// 2. 按 Key 排序
	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	// 3. 输出文件（仍然使用临时文件 + Rename）
	outName := fmt.Sprintf("mr-out-%d", reduceTaskID)
	tmpFile, err := ioutil.TempFile("", fmt.Sprintf("mr-out-%d-tmp", reduceTaskID))
	if err != nil {
		return fmt.Errorf("create temp output file: %v", err)
	}

	// 4. 聚合相同 Key，调用 reducef
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(tmpFile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	tmpFile.Close()

	if err := os.Rename(tmpFile.Name(), outName); err != nil {
		os.Remove(tmpFile.Name())
		return fmt.Errorf("rename temp output %s to %s: %v", tmpFile.Name(), outName, err)
	}

	log.Printf("Worker %s: Finish ReduceTask %d (Output=%s)",
		workerID, reduceTaskID, outName)
	return nil
}

// 上报任务完成
func reportTaskDone(workerID string, taskType TaskType, taskID int, intermediateFiles []string) {
	req := TaskDoneRequest{
		TaskType:          taskType,
		TaskID:            taskID,
		IntermediateFiles: intermediateFiles,
	}
	resp := TaskDoneResponse{}
	if ok := call("Coordinator.ReportTaskDone", &req, &resp); ok {
		if resp.Success {
			log.Printf("Worker %s: Report %s %d done success",
				workerID, taskTypeStr(taskType), taskID)
		} else {
			log.Printf("Worker %s: Report %s %d done failed: %s",
				workerID, taskTypeStr(taskType), taskID, resp.ErrMsg)
		}
	} else {
		log.Printf("Worker %s: Report %s %d done failed (Coordinator disconnected)",
			workerID, taskTypeStr(taskType), taskID)
	}
}

// TaskType → 字符串（仅用于日志）
func taskTypeStr(t TaskType) string {
	switch t {
	case MapTask:
		return "MapTask"
	case ReduceTask:
		return "ReduceTask"
	case ExitTask:
		return "ExitTask"
	default:
		return "UnknownTask"
	}
}

// call: Worker 侧统一的 RPC 封装
func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Printf("dialing %s: %v", sockname, err)
		return false
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err != nil {
		log.Printf("call %s error: %v", rpcname, err)
		return false
	}
	return true
}
