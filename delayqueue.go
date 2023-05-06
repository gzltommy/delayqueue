package delayqueue

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

// DelayQueue 延时任务对象
type DelayQueue struct {
	tasks                 []*task             // 存储任务列表的切片
	add                   chan *task          // 用户添加任务的管道信号
	remove                chan string         // 用户删除任务的管道信号
	waitRemoveTaskMapping map[string]struct{} // 等待删除的任务 id 列表
}

// task 任务对象
type task struct {
	id       string    // 任务id
	execTime time.Time // 执行时间
	f        func()    // 执行函数
}

// NewDelayQueue 创建延时任务队列对象
func NewDelayQueue() *DelayQueue {
	q := &DelayQueue{
		add:                   make(chan *task, 10000),
		remove:                make(chan string, 100),
		waitRemoveTaskMapping: make(map[string]struct{}),
	}

	// 开启协程，监听任务相关信号
	go q.start()
	return q
}

// Delete 用户删除任务
func (q *DelayQueue) Delete(id string) {
	q.remove <- id
}

// Push 用户推送任务
func (q *DelayQueue) Push(timeInterval time.Duration, f func()) string {
	// 生成一个任务id，方便删除使用
	id := genTaskId()
	t := &task{
		id:       id,
		execTime: time.Now().Add(timeInterval),
		f:        f,
	}

	// 将任务推到 add 管道中
	q.add <- t
	return id
}

// start 监听各种任务相关信号
func (q *DelayQueue) start() {
	for {
		if len(q.tasks) == 0 {
			// 任务列表为空的时候，只需要监听 add 管道
			select {
			case t := <-q.add:
				// 添加任务
				q.addTask(t)
			}
			continue
		}

		// 任务列表不为空的时候，需要监听所有管道

		// 任务的等待时间 = 任务的执行时间 - 当前的时间
		currentTask := q.tasks[0]
		timer := time.NewTimer(currentTask.execTime.Sub(time.Now()))

		select {
		case now := <-timer.C:
			timer.Stop()
			if _, isRemove := q.waitRemoveTaskMapping[currentTask.id]; isRemove {
				// 之前客户已经发出过该任务的删除信号，因此需要结束任务，刷新任务列表
				q.endTask()
				delete(q.waitRemoveTaskMapping, currentTask.id)
				continue
			}

			// 开启协程，异步执行任务
			go q.execTask(currentTask, now)

			// 任务结束，刷新任务列表
			q.endTask()
		case tsk := <-q.add:
			// 添加任务
			timer.Stop()
			q.addTask(tsk)
		case id := <-q.remove:
			// 删除任务
			timer.Stop()
			q.deleteTask(id)
		}
	}
}

// execTask 执行任务
func (q *DelayQueue) execTask(task *task, currentTime time.Time) {
	if task.execTime.After(currentTime) {
		// 如果当前任务的执行时间落后于当前时间，则不执行
		return
	}

	// 执行任务
	task.f()
	return
}

// endTask 一个任务去执行了，刷新任务列表
func (q *DelayQueue) endTask() {
	if len(q.tasks) == 1 {
		q.tasks = []*task{}
		return
	}

	q.tasks = q.tasks[1:]
}

// addTask 将任务添加到任务切片列表中
func (q *DelayQueue) addTask(t *task) {
	// 寻找新增任务的插入位置
	insertIndex := q.getTaskInsertIndex(t, 0, len(q.tasks)-1)
	// 找到了插入位置，更新任务列表
	q.tasks = append(q.tasks, &task{})
	copy(q.tasks[insertIndex+1:], q.tasks[insertIndex:])
	q.tasks[insertIndex] = t
}

// deleteTask 删除指定任务
// FIXME:注意，这里暂时不考虑，任务 id 非法的特殊情况
func (q *DelayQueue) deleteTask(id string) {
	deleteIndex := -1
	for index, t := range q.tasks {
		if t.id == id {
			// 找到了在切片中需要删除的索引
			deleteIndex = index
			break
		}
	}

	if deleteIndex == -1 {
		// 如果没有找到删除的任务，说明任务还在 add 管道中，来不及更新到 tasks 中，这里我们就将这个删除 id 临时记录下来
		// FIXME:注意，这里暂时不考虑，任务 id 非法的特殊情况
		q.waitRemoveTaskMapping[id] = struct{}{}
		return
	}

	if len(q.tasks) == 1 {
		// 删除后，任务列表就没有任务了
		q.tasks = []*task{}
		return
	}

	if deleteIndex == len(q.tasks)-1 {
		// 如果删除的是，任务列表的最后一个元素，则执行下列代码
		q.tasks = q.tasks[:len(q.tasks)-1]
		return
	}

	// 如果删除的是，任务列表的其他元素，则需要将 deleteIndex 之后的元素，全部向前挪动一位
	copy(q.tasks[deleteIndex:len(q.tasks)-1], q.tasks[deleteIndex+1:len(q.tasks)-1])
	q.tasks = q.tasks[:len(q.tasks)-1]
	return
}

// getTaskInsertIndex 寻找任务的插入位置
func (q *DelayQueue) getTaskInsertIndex(t *task, leftIndex, rightIndex int) (index int) {
	// 使用二分法判断新增任务的插入位置
	if len(q.tasks) == 0 {
		return
	}

	length := rightIndex - leftIndex
	if q.tasks[leftIndex].execTime.Sub(t.execTime) >= 0 {
		// 如果当前切片中最小的元素都超过了插入的优先级，则插入位置应该是最左边
		return leftIndex
	}

	if q.tasks[rightIndex].execTime.Sub(t.execTime) <= 0 {
		// 如果当前切片中最大的元素都没超过插入的优先级，则插入位置应该是最右边
		return rightIndex + 1
	}

	if length == 1 && q.tasks[leftIndex].execTime.Before(t.execTime) &&
		q.tasks[rightIndex].execTime.Sub(t.execTime) >= 0 {
		// 如果插入的优先级刚好在仅有的两个优先级之间，则中间的位置就是插入位置
		return leftIndex + 1
	}

	middleVal := q.tasks[leftIndex+length/2].execTime

	// 这里用二分法递归的方式，一直寻找正确的插入位置
	if t.execTime.Sub(middleVal) <= 0 {
		return q.getTaskInsertIndex(t, leftIndex, leftIndex+length/2)
	} else {
		return q.getTaskInsertIndex(t, leftIndex+length/2, rightIndex)
	}
}

func genTaskId() string {
	return primitive.NewObjectID().Hex()
}
