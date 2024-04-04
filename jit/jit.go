// jit implements just in time compilation of expressions, this requires a not
// to neglect start up time and thus should only be invoked once the callee is
// sure to outperform the interpreted expression using the compiled function
package jit

import (
	"errors"
	"runtime"
)

// Jit invokes the code generation, calls the go compiler, opens the compiled
// plugin and returns the generated and compiled function
func Jit() (func(), error) {
	if runtime.GOOS == "windows" {
		return nil, errors.New(`
The go plugin api is not supported on windows, just in time compilation is therefore not available.
See: https://pkg.go.dev/plugin#hdr-Warnings
`)
	}
	return nil, nil
}
