package mr

import (
	"encoding/gob"
	"os"
	"strconv"
)

// 初始化结构体注册（Go RPC 不强制，但注册一下更稳）
func init() {
	gob.Register(TaskRequest{})
	gob.Register(TaskResponse{})
	gob.Register(TaskDoneRequest{})
	gob.Register(TaskDoneResponse{})
}

// --------------------------
// 1. 任务类型枚举
// --------------------------
type TaskType int

const (
	NoTask     TaskType = 0 // 无可用任务（Worker 暂时等待）
	MapTask    TaskType = 1 // Map 任务
	ReduceTask TaskType = 2 // Reduce 任务
	ExitTask   TaskType = 3 // 所有任务完成，Worker 可以退出
)

// --------------------------
// 2. 任务状态常量（Coordinator 内部使用）
// --------------------------
const (
	TaskIdle       = "idle"       // 任务未分配
	TaskInProgress = "inProgress" // 任务执行中
	TaskDone       = "done"       // 任务已完成
)

// --------------------------
// 3. RPC 结构体定义
// --------------------------

// Worker 向 Coordinator 请求任务
type TaskRequest struct {
	WorkerID string // 可选：Worker 唯一标识（PID 等）
}

// Coordinator 返回的任务描述
type TaskResponse struct {
	Type TaskType

	// Map 任务专属字段
	MapTaskID int
	InputFile string
	NReduce   int

	// Reduce 任务专属字段
	ReduceTaskID  int
	AllMapTaskIDs []int
}

// Worker 向 Coordinator 上报任务完成
type TaskDoneRequest struct {
	TaskType          TaskType
	TaskID            int
	IntermediateFiles []string // 可选：Map 生成的中间文件（目前未用，仅调试）
}

// Coordinator 对完成上报的回复
type TaskDoneResponse struct {
	Success bool
	ErrMsg  string
}

// coordinatorSock: Coordinator 使用的 UNIX 域 socket 路径
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
