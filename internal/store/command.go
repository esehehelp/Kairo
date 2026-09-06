package store

func ackState(phase string) (string, bool) {
	switch phase {
	case "accepted", "checkpointing", "checkpointed", "rejected":
		return phase, true
	default:
		return "", false
	}
}

func commandStateRank(state string) int {
	switch state {
	case "pending":
		return 0
	case "accepted":
		return 1
	case "checkpointing":
		return 2
	case "checkpointed":
		return 3
	case "rejected":
		return 4
	default:
		return -1
	}
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}
