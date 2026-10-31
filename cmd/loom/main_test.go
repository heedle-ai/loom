package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestCompileCommand(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), []string{"compile", `(pipe (echo "a") echo)`}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"requires": [`) {
		t.Errorf("unexpected plan:\n%s", out.String())
	}
	if err := run(context.Background(), []string{"compile", `(pipe (echo "a") shout)`}, &out); err == nil ||
		!strings.Contains(err.Error(), `unknown builtin "shout"`) {
		t.Errorf("err = %v, want unknown builtin", err)
	}
	for _, bad := range [][]string{nil, {"bogus"}, {"compile"}, {"worker"}} {
		if err := run(context.Background(), bad, &out); err == nil {
			t.Errorf("run(%v) succeeded, want usage error", bad)
		}
	}
}
