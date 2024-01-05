package stats

import (
	"github.com/c9s/goprocinfo/linux"
	"github.com/rs/zerolog/log"
)

// Machine stats
type Stats struct {
	MemoryStats *linux.MemInfo
	DiskStats   *linux.Disk
	CpuStats    *linux.CPUStat
	LoadStats   *linux.LoadAvg
}

func (s *Stats) MemTotalKb() uint64 {
	return s.MemoryStats.MemTotal
}

func (s *Stats) MemAvailableKb() uint64 {
	return s.MemoryStats.MemAvailable
}

func (s *Stats) MemUsedKb() uint64 {
	return s.MemoryStats.MemTotal - s.MemoryStats.MemAvailable
}

func (s *Stats) MemUsedPercent() uint64 {
	return s.MemoryStats.MemAvailable / s.MemoryStats.MemTotal
}

func (s *Stats) DiskTotal() uint64 {
	return s.DiskStats.All
}

func (s *Stats) DiskFree() uint64 {
	return s.DiskStats.Free
}

func (s *Stats) DiskUsed() uint64 {
	return s.DiskStats.Used
}

func (s *Stats) CpuUsage() float64 {
	idle := s.CpuStats.Idle + s.CpuStats.IOWait
	active := s.CpuStats.User + s.CpuStats.Nice + s.CpuStats.System + s.CpuStats.IRQ + s.CpuStats.SoftIRQ + s.CpuStats.Steal
	total := idle + active
	if total == 0 {
		return 0
	}
	return (float64(total) - float64(idle)) / float64(total)
}

// Get the machine stats
func GetStats() *Stats {
	return &Stats{
		MemoryStats: getMemoryInfo(),
		DiskStats:   getDiskInfo(),
		CpuStats:    getCpuStats(),
		LoadStats:   getLoadAvg(),
	}
}

func getMemoryInfo() *linux.MemInfo {
	memstats, err := linux.ReadMemInfo("/proc/meminfo")
	if err != nil {
		log.Err(err).Msg("error reading from /proc/meminfo")
		return &linux.MemInfo{}
	}
	return memstats
}

func getDiskInfo() *linux.Disk {
	diskstats, err := linux.ReadDisk("/")
	if err != nil {
		log.Err(err).Msg("error reading from /")
		return &linux.Disk{}
	}
	return diskstats
}

func getCpuStats() *linux.CPUStat {
	stats, err := linux.ReadStat("/proc/stat")
	if err != nil {
		log.Err(err).Msg("error reading from /proc/stat")
		return &linux.CPUStat{}
	}
	return &stats.CPUStatAll
}

func getLoadAvg() *linux.LoadAvg {
	loadavg, err := linux.ReadLoadAvg("/proc/loadavg")
	if err != nil {
		log.Err(err).Msg("error reading from /proc/loadavg")
		return &linux.LoadAvg{}
	}
	return loadavg
}
