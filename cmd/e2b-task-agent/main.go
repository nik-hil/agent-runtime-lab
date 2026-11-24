// e2b-task-agent is cross-compiled from source and runs only inside the E2B VM.
// It is not the fleet daemon and must never be used to execute tenant work on
// the orchestration host. A published manifest is observational evidence only.
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"agent-runtime-lab/src/taskevidence"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "task evidence unavailable:", err)
		os.Exit(1)
	}
}

func run() error {
	encoded := flag.String("request-base64", "", "bounded fleetd-generated task request")
	flag.Parse()
	if flag.NArg() != 0 || *encoded == "" || len(*encoded) > 60<<10 {
		return errors.New("one bounded --request-base64 required")
	}
	data, err := base64.RawURLEncoding.DecodeString(*encoded)
	if err != nil {
		return errors.New("invalid task request encoding")
	}
	var request taskevidence.Request
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return errors.New("invalid task request")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("trailing task request data")
	}
	if err := request.Validate(); err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	helper, err := os.ReadFile(executable)
	if err != nil || taskevidence.Hash(helper) != request.Identity.Intent.HelperSHA256 {
		return errors.New("helper binary does not match durable intent")
	}
	// The running Linux executable remains mapped after unlink. Keep no
	// unnecessary helper copies after upload; the completion directory stays.
	if err := os.Remove(executable); err != nil {
		return err
	}
	if _, err := taskevidence.Execute(context.Background(), taskevidence.Root, request); err != nil {
		return err
	}
	fmt.Println("M43_COMPLETION_RECORDED")
	return nil
}
