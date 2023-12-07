package task

type State int

const (
	Pending State = iota
	Scheduled
	Running
	Completed
	Failed
)

var stateTransitionMap = map[State][]State{
	Pending:   {Scheduled},
	Scheduled: {Running, Failed},
	Running:   {Completed, Failed},
	Completed: {},
	Failed:    {},
}

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
