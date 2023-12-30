package node

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"orchestrator/stats"
)

type Node struct {
	Name            string
	Api             string
	Role            string
	Stats           stats.Stats
	Memory          int64
	MemoryAllocated int64
	Disk            int64
	DiskAllocated   int64
	TaskCount       int
}

func NewNode(name string, api string, role string) Node {
	return Node{
		Name: name,
		Api:  api,
		Role: role,
	}
}

func (n *Node) UpdateStats() error {
	var resp *http.Response
	var err error

	url := fmt.Sprintf("%s/metrics", n.Api)
	resp, err = http.Get(url)
	if err != nil {
		return fmt.Errorf("unable to connect to %v", n.Api)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("encountered unexpected http code retrieving stats from %s: %v, err: %v", n.Api, resp.StatusCode, err)
	}

	body, _ := io.ReadAll(resp.Body)
	var stats stats.Stats
	err = json.Unmarshal(body, &stats)
	if err != nil {
		return fmt.Errorf("error decoding message while getting stats for node %s", n.Name)
	}

	if stats.MemoryStats == nil || stats.DiskStats == nil {
		return fmt.Errorf("error getting stats from node %s", n.Name)
	}

	n.Memory = int64(stats.MemTotalKb())
	n.MemoryAllocated = int64(stats.MemUsedKb())
	n.Disk = int64(stats.DiskTotal())
	n.DiskAllocated = int64(stats.DiskUsed())
	n.Stats = stats

	return nil
}
