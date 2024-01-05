package scheduler

import (
	"orchestrator/node"
	"orchestrator/task"
)

// Selector of worker node to run a task
type Scheduler interface {
	// Select the most suitable worker node to run the given task
	SelectNode(t task.Task, nodes []*node.Node) *node.Node
}
