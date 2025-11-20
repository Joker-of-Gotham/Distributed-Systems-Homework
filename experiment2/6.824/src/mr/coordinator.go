package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Map 任务元信息
type MapTaskInfo struct {
	ID         int       // Map 任务编号
	InputFile  string    // 输入文件
	Status     string    // 任务状态：idle / inProgress / done
	AssignTime time.Time // 最近一次分配时间（用于超时）
}

// Reduce 任务元信息
type ReduceTaskInfo struct {
	ID         int
	Status     string
	AssignTime time.Time
}

// Coordinator 结构体
type Coordinator struct {
	mu sync.Mutex

	nReduce int

	mapTasks    []MapTaskInfo
	reduceTasks []ReduceTaskInfo

	mapDoneCount    int // 已完成的 Map 任务数
	reduceDoneCount int // 已完成的 Reduce 任务数

	isDone bool // 所有 Map + Reduce 任务是否全部完成
}

// AssignTask: Worker RPC 调用，申请一个任务
func (c *Coordinator) AssignTask(args *TaskRequest, reply *TaskResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 如果已经判定所有任务完成，直接让 Worker 退出
	if c.isDone {
		reply.Type = ExitTask
		return nil
	}

	// --------------------------
	// 优先处于 Map 阶段：如果还有未完成的 Map 任务
	// --------------------------
	if c.mapDoneCount < len(c.mapTasks) {
		// 尽量分配一个空闲的 Map 任务
		for i, task := range c.mapTasks {
			if task.Status == TaskIdle {
				c.mapTasks[i].Status = TaskInProgress
				c.mapTasks[i].AssignTime = time.Now()

				reply.Type = MapTask
				reply.MapTaskID = task.ID
				reply.InputFile = task.InputFile
				reply.NReduce = c.nReduce

				log.Printf("Assign MapTask: ID=%d, File=%s", task.ID, task.InputFile)
				return nil
			}
		}

		// 没有空闲 Map 任务（要么都在跑，要么都 done）但 mapDoneCount 还没满：
		// 表示当前只有“等待其他 Worker 完成”的事，先让 Worker 休息一下再来
		reply.Type = NoTask
		return nil
	}

	// --------------------------
	// Map 阶段已全部完成，进入 Reduce 阶段
	// --------------------------
	if c.reduceDoneCount < len(c.reduceTasks) {
		// 尽量分配一个空闲的 Reduce 任务
		for i, task := range c.reduceTasks {
			if task.Status == TaskIdle {
				c.reduceTasks[i].Status = TaskInProgress
				c.reduceTasks[i].AssignTime = time.Now()

				// 所有 Map 任务都已 done，可以把全部 MapID 交给 Reduce
				allMapIDs := make([]int, 0, len(c.mapTasks))
				for _, mt := range c.mapTasks {
					allMapIDs = append(allMapIDs, mt.ID)
				}

				reply.Type = ReduceTask
				reply.ReduceTaskID = task.ID
				reply.AllMapTaskIDs = allMapIDs

				log.Printf("Assign ReduceTask: ID=%d", task.ID)
				return nil
			}
		}

		// 没有空闲 Reduce 任务（要么在跑，要么 done），但 reduceDoneCount 还没满：
		// 表示还有 Reduce 正在执行，Worker 暂时等待
		reply.Type = NoTask
		return nil
	}

	// --------------------------
	// Map + Reduce 已全部完成
	// --------------------------
	reply.Type = ExitTask
	return nil
}

// ReportTaskDone: Worker RPC 调用，汇报任务完成
func (c *Coordinator) ReportTaskDone(args *TaskDoneRequest, reply *TaskDoneResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case MapTask:
		if args.TaskID < 0 || args.TaskID >= len(c.mapTasks) {
			reply.Success = false
			reply.ErrMsg = fmt.Sprintf("Invalid MapTaskID: %d", args.TaskID)
			return nil
		}
		task := &c.mapTasks[args.TaskID]
		if task.Status != TaskDone {
			task.Status = TaskDone
			c.mapDoneCount++
			log.Printf("MapTask Done: ID=%d (mapDone=%d/%d)",
				args.TaskID, c.mapDoneCount, len(c.mapTasks))
		} else {
			// 可能是重试任务的重复上报，忽略但认为成功
			log.Printf("MapTask Done duplicate report: ID=%d", args.TaskID)
		}

	case ReduceTask:
		if args.TaskID < 0 || args.TaskID >= len(c.reduceTasks) {
			reply.Success = false
			reply.ErrMsg = fmt.Sprintf("Invalid ReduceTaskID: %d", args.TaskID)
			return nil
		}
		task := &c.reduceTasks[args.TaskID]
		if task.Status != TaskDone {
			task.Status = TaskDone
			c.reduceDoneCount++
			log.Printf("ReduceTask Done: ID=%d (reduceDone=%d/%d)",
				args.TaskID, c.reduceDoneCount, len(c.reduceTasks))
		} else {
			log.Printf("ReduceTask Done duplicate report: ID=%d", args.TaskID)
		}

	default:
		reply.Success = false
		reply.ErrMsg = fmt.Sprintf("Unknown TaskType: %d", args.TaskType)
		return nil
	}

	// 所有任务完成 → 标记 isDone = true
	if c.mapDoneCount == len(c.mapTasks) && c.reduceDoneCount == len(c.reduceTasks) {
		if !c.isDone {
			c.isDone = true
			log.Printf("All tasks finished: %d map, %d reduce", c.mapDoneCount, c.reduceDoneCount)
		}
	}

	reply.Success = true
	reply.ErrMsg = ""
	return nil
}

// MakeCoordinator: 初始化 Coordinator
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
		isDone:  false,
	}

	// 初始化 Map 任务
	c.mapTasks = make([]MapTaskInfo, len(files))
	for i, file := range files {
		c.mapTasks[i] = MapTaskInfo{
			ID:         i,
			InputFile:  file,
			Status:     TaskIdle,
			AssignTime: time.Time{},
		}
	}

	// 初始化 Reduce 任务
	c.reduceTasks = make([]ReduceTaskInfo, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = ReduceTaskInfo{
			ID:         i,
			Status:     TaskIdle,
			AssignTime: time.Time{},
		}
	}

	// 启动超时检测协程
	go c.checkTaskTimeout()

	// 启动 RPC 服务
	c.server()
	return &c
}

// 定期检查任务是否超时（>10s 未完成则重置为 idle）
func (c *Coordinator) checkTaskTimeout() {
	for {
		time.Sleep(1 * time.Second)

		c.mu.Lock()

		// 如果已经全部完成，退出协程
		if c.isDone {
			c.mu.Unlock()
			return
		}

		now := time.Now()
		// 检查 Map 任务超时
		for i, task := range c.mapTasks {
			if task.Status == TaskInProgress && now.Sub(task.AssignTime) > 10*time.Second {
				log.Printf("MapTask Timeout: ID=%d, reset to idle", task.ID)
				c.mapTasks[i].Status = TaskIdle
			}
		}
		// 检查 Reduce 任务超时
		for i, task := range c.reduceTasks {
			if task.Status == TaskInProgress && now.Sub(task.AssignTime) > 10*time.Second {
				log.Printf("ReduceTask Timeout: ID=%d, reset to idle", task.ID)
				c.reduceTasks[i].Status = TaskIdle
			}
		}

		c.mu.Unlock()
	}
}

// RPC server 启动逻辑（保持原样）
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	log.Printf("Coordinator RPC server started on %s", sockname)
}

// Done: 供 main/mrcoordinator.go 周期性轮询
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.isDone
}
