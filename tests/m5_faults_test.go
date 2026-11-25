package tests

// Share the actual fault arguments between the live subprocess fixture and
// the CI protocol fixture. Only the initial owner may hold pending-move;
// failover destinations must remain able to claim it.
func m5PendingFaultArgs(nodeID string) []string {
	if nodeID == "node-a" {
		return []string{"--test-hold-claim-job", "pending-move"}
	}
	return nil
}
