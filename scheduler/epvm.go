package scheduler

import (
	"math"
	"orchestrator/node"
	"orchestrator/task"
	"time"

	"github.com/rs/zerolog/log"
)

// LIEB square ice constant
const LIEB = 1.53960071783900203869

// Scheduler which computes a score based on the worker's current system load statistics
// to pick the most suitable worker for the given task
type Epvm struct{}

func (e *Epvm) SelectNode(t task.Task, nodes []*node.Node) *node.Node {
	candidates := e.selectCandidateNodes(t, nodes)
	if len(candidates) == 0 {
		return nil
	}
	scores := e.score(t, candidates)
	return e.pick(scores, candidates)
}

// Get suitable worker nodes to run the given task, based on the disk space requirement
func (e *Epvm) selectCandidateNodes(t task.Task, nodes []*node.Node) []*node.Node {
	var candidates []*node.Node
	for node := range nodes {
		if checkDisk(t, nodes[node].Disk-nodes[node].DiskAllocated) {
			candidates = append(candidates, nodes[node])
		}
	}
	return candidates
}

func (e *Epvm) score(t task.Task, nodes []*node.Node) map[string]float64 {
	if len(nodes) == 0 {
		return nil
	}
	nodeScores := make(map[string]float64)
	maxJobs := 4.0

	for _, node := range nodes {
		err := node.UpdateStats()
		if err != nil {
			log.Err(err).Str("node", node.Name).Msg("failed to update node stats")
			continue
		}

		cpuUsage, err := calculateAvgCpuUsage(node, node.Stats.CpuUsage())
		if err != nil {
			log.Err(err).Str("node", node.Name).Msg("error calculating node CPU usage")
			continue
		}
		cpuLoad := calculateLoad(cpuUsage, math.Pow(2, 0.8))

		memoryAllocated := float64(node.Stats.MemUsedKb()) + float64(node.MemoryAllocated)
		memoryPercentAllocated := memoryAllocated / float64(node.Memory)

		newMemPercent := calculateLoad(memoryAllocated+float64(t.Memory/1000), float64(node.Memory))
		memCost := math.Pow(LIEB, newMemPercent) + math.Pow(LIEB, float64(node.TaskCount+1)/maxJobs) - math.Pow(LIEB, memoryPercentAllocated) - math.Pow(LIEB, float64(node.TaskCount)/float64(maxJobs))
		cpuCost := math.Pow(LIEB, cpuLoad) + math.Pow(LIEB, float64(node.TaskCount+1)/maxJobs) - math.Pow(LIEB, cpuLoad) - math.Pow(LIEB, float64(node.TaskCount)/float64(maxJobs))

		nodeScores[node.Name] = memCost + cpuCost
	}
	return nodeScores
}

func (e *Epvm) pick(scores map[string]float64, candidates []*node.Node) *node.Node {
	if len(candidates) == 0 {
		return nil
	}

	minCost := scores[candidates[0].Name]
	bestNode := candidates[0]
	for i := 1; i < len(candidates); i++ {
		node := candidates[i]
		if scores[node.Name] < minCost {
			minCost = scores[node.Name]
			bestNode = node
		}
	}
	return bestNode
}

func checkDisk(t task.Task, diskAvailable int64) bool {
	return t.Disk <= diskAvailable
}

func calculateLoad(usage float64, capacity float64) float64 {
	return usage / capacity
}

// Calculate CPU usage by sampling 2 times for an average
func calculateAvgCpuUsage(node *node.Node, initialCpuUsage float64) (float64, error) {
	time.Sleep(time.Second)
	err := node.UpdateStats()
	if err != nil {
		return 0, err
	}
	cpuUsage := node.Stats.CpuUsage()

	avgUsage := (initialCpuUsage + cpuUsage) / 2
	return avgUsage, nil
}
