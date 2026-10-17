// Run from an explicitly selected lenso-go checkout; no checkout files are edited.
package main

import (
	"context"
	"errors"
	"fmt"
	"lenso.local/go/sdk/runtime"
	"os"
	"time"
)

func contract(version, method string) *runtime.Contract {
	c, err := runtime.ParseContract([]byte(fmt.Sprintf(`{"version":%q,"methods":{%q:{"mode":"unary","input":{"type":"integer"},"output":{"type":"null"},"error":{"type":"null"}}}}`, version, method)))
	if err != nil {
		panic(err)
	}
	return c
}
func check(err error, code string) {
	var failure *runtime.Error
	if !errors.As(err, &failure) || failure.Code != code || len(failure.Details) != 0 {
		panic(fmt.Sprintf("expected %s: %v", code, err))
	}
	fmt.Printf("Go %s: passed\n", code)
}
func main() {
	ctx := context.Background()
	empty, err := runtime.ParseContract([]byte(`{"version":"1","methods":{}}`))
	if err != nil {
		panic(err)
	}
	host, err := runtime.Connect(ctx, runtime.Options{Contract: empty, Address: "127.0.0.1:" + os.Getenv("LENSO_RUNTIME_PORT"), Identity: runtime.Identity{Namespace: "errors", Service: "go", Instance: "one"}})
	if err != nil {
		panic(err)
	}
	defer host.Close(ctx)
	_, err = runtime.Connect(ctx, runtime.Options{Contract: empty, Address: "127.0.0.1:" + os.Getenv("LENSO_PROTOCOL_PORT"), Identity: runtime.Identity{Namespace: "errors", Service: "bad-go", Instance: "one"}})
	check(err, "protocol_error")
	target := runtime.Target{Namespace: "errors", Service: "server"}
	_, err = runtime.Call[int, any](ctx, host, target, contract("2", "wait"), "wait", 0)
	check(err, "contract_mismatch")
	_, err = runtime.Call[int, any](ctx, host, target, contract("1", "missing"), "missing", 0)
	check(err, "contract_mismatch")
	_, err = runtime.Call[int, any](ctx, host, target, contract("1", "wait"), "wait", 500, runtime.CallOptions{Timeout: 50 * time.Millisecond})
	check(err, "deadline_exceeded")
	if !errors.Is(err, context.DeadlineExceeded) {
		panic("deadline compatibility")
	}
	cancelled, cancel := context.WithCancel(ctx)
	timer := time.AfterFunc(50*time.Millisecond, cancel)
	_, err = runtime.Call[int, any](cancelled, host, target, contract("1", "wait"), "wait", 500)
	timer.Stop()
	cancel()
	check(err, "cancelled")
	if !errors.Is(err, context.Canceled) {
		panic("cancellation compatibility")
	}
	_, err = runtime.Call[int, any](ctx, host, target, contract("1", "wait"), "wait", 0)
	if err != nil {
		panic(err)
	}
}
