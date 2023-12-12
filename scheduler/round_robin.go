package scheduler

import (
	"orchestrator/node"
	"orchestrator/task"
)

type RoundRobin struct {
	LastWorker int
}

func (r *RoundRobin) SelectCandidateNodes(t task.Task, nodes []*node.Node) []*node.Node {
	return nodes
}

func (r *RoundRobin) Score(t task.Task, nodes []*node.Node) map[string]float64 {
	if len(nodes) == 0 {
		return nil
	}
	var newWorker int
	if r.LastWorker == len(nodes)-1 {
		newWorker = 0
	} else {
		newWorker = r.LastWorker + 1
	}
	r.LastWorker = newWorker

	scores := make(map[string]float64, len(nodes))
	for i, node := range nodes {
		if i == newWorker {
			scores[node.Name] = 0.1
		} else {
			scores[node.Name] = 1
		}
	}
	return scores
}

func (r *RoundRobin) Pick(scores map[string]float64, nodes []*node.Node) *node.Node {
	if len(nodes) == 0 {
		return nil
	}

	lowest := scores[nodes[0].Name]
	selected := nodes[0]
	for _, node := range nodes {
		if scores[node.Name] < lowest {
			lowest = scores[node.Name]
			selected = node
		}
	}
	return selected
}
