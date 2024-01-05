package scheduler

import (
	"orchestrator/node"
	"orchestrator/task"
)

// Simple scheduler that selects the next available worker
//
// After the last worker is selected, it goes back to the first one
type RoundRobin struct {
	LastWorkerNode int
}

func (r *RoundRobin) SelectNode(t task.Task, nodes []*node.Node) *node.Node {
	if len(nodes) == 0 {
		return nil
	}

	var newWorker int
	if r.LastWorkerNode == len(nodes)-1 {
		newWorker = 0
	} else {
		newWorker = r.LastWorkerNode + 1
	}
	r.LastWorkerNode = newWorker
	return nodes[newWorker]
}
