package task

// State of a task
type State int

const (
	Pending   State = iota // The task is be be scheduled
	Scheduled              // The task will be executed on a worker node
	Running                // The task is running on a worker node
	Completed              // The task is no longer running, it was successfully stopped
	Failed                 // The task execution failed
)

// Allowed state transitions
var stateTransitionMap = map[State][]State{
	Pending:   {Scheduled},
	Scheduled: {Running, Failed},
	Running:   {Completed, Failed, Scheduled}, // Scheduled is included for tasks restart
	Completed: {},
	Failed:    {Scheduled},
}

// Verify if a state transition is legal
func ValidStateTransition(current, target State) bool {
	if current == target {
		return true
	}
	for _, s := range stateTransitionMap[current] {
		if s == target {
			return true
		}
	}
	return false
}
