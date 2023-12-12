package scheduler

import (
	"orchestrator/node"
	"orchestrator/task"
)

type Scheduler interface {
	SelectCandidateNodes(t task.Task, nodes []*node.Node) []*node.Node
	Score(t task.Task, nodes []*node.Node) map[string]float64
	Pick(scores map[string]float64, nodes []*node.Node) *node.Node
}
