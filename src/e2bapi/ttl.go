package e2bapi

import (
	"context"
	"errors"
	"net/http"
)

// SetTimeout extends a *known* sandbox's life from now. This is an explicit,
// opt-in operation: the background audit never calls it. It must not be used
// for an unknown provider identity or a terminal/deleting workload.
func (b *Backend) SetTimeout(ctx context.Context, id string, seconds int) error {
	if seconds < 30 || seconds > 3600 {
		return errors.New("sandbox TTL must be 30..3600 seconds")
	}
	path, err := sandboxPath(id)
	if err != nil {
		return err
	}
	_, _, err = b.do(ctx, http.MethodPost, path+"/timeout", map[string]int{"timeout": seconds}, http.StatusNoContent)
	return err
}
