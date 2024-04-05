// jit implements just in time compilation of expressions, this requires a not
// to neglect start up time and thus should only be invoked once the callee is
// sure to outperform the interpreted expression using the compiled function
package jit

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"

	"github.com/hneemann/parser2"
)

// Jit invokes the code generation, traverses the abstract syntax tree, calls
// the go compiler, opens the compiled plugin and returns the generated and
// compiled function
func Jit(ast parser2.AST) (Function, error) {
	if runtime.GOOS == "windows" {
		return nil, fmt.Errorf(`
The go plugin api is not supported on windows, just in time compilation is therefore not available.
See: https://pkg.go.dev/plugin#hdr-Warnings (%w)`, errors.ErrUnsupported)
	}

	s := Stencil{
		Name:          "Cos",
		ParameterName: "n",
	}

	f, err := os.CreateTemp(".", "jit_*.go")
	defer os.Remove(f.Name())
	if err != nil {
		return nil, err
	}
	err = generate(f, s, ast)
	if err != nil {
		return nil, err
	}

	path, err := compile(f.Name())
	if err != nil {
		return nil, err
	}
	defer os.Remove(path)

	return function(s.Name, path)
}

// compile invokes the go compiler to create a shared object / go plugin from
// the go file at the specifed path
func compile(path string) (soPath string, err error) {
	soPath = strings.Replace(path, ".go", ".so", 1)
	cmd := exec.Command("go", "build", "-buildmode=plugin", "-o", soPath, path)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	err = cmd.Run()
	return
}
