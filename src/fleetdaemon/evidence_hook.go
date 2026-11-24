package fleetdaemon

import (
	"context"

	"agent-runtime-lab/src/taskevidence"
)

// EvidenceHookExecutor is an opt-in test decorator. The hook is called only
// after a valid VM completion envelope is read, before TaskHandler can persist
// the result. Normal task submission, cached replay and recovery do not need it.
// fleetd uses it only for its explicitly named test-crash target.
type EvidenceHookExecutor struct {
	Executor
	EvidenceExecutor
	AfterVerified func(string)
}

func (e EvidenceHookExecutor) RunEvidence(ctx context.Context, identity taskevidence.Identity, command string) (taskevidence.Envelope, error) {
	proof, err := e.EvidenceExecutor.RunEvidence(ctx, identity, command)
	if err == nil {
		err = proof.Validate(identity)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err == nil && e.AfterVerified != nil {
		e.AfterVerified(identity.TaskID)
	}
	return proof, err
}
