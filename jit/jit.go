// jit implements just in time compilation of expressions, this requires a not
// to neglect start up time and thus should only be invoked once the callee is
// sure to outperform the interpreted expression using the compiled function
package jit

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"plugin"
	"runtime"
	"strings"

	"github.com/hneemann/parser2"
)

// Jit works by assuming perfect code and state, if the compilation failes due
// to a compilation error the Jit bails out of execution
type Jit[V any] struct{}

const id_length = 15

var set = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ123456789")

func id() string {
	r := make([]byte, id_length)
	for i := 0; i < id_length; i++ {
		r[i] = set[rand.IntN(len(set))]
	}
	return string(r)
}

// TODO: this should probably use a context to time out after 2 seconds?

// Invokes the code generation, traverses the abstract syntax tree, calls
// the go compiler, opens the compiled plugin and returns the generated and
// compiled function
func (j *Jit[V]) Compile(ast parser2.AST, params []string) (func(V) (V, error), error) {
	if runtime.GOOS == "windows" {
		return nil, fmt.Errorf(`
The go plugin api is not supported on windows, just in time compilation is therefore not available.
See: https://pkg.go.dev/plugin#hdr-Warnings (%w)`, errors.ErrUnsupported)
	}

	name := "Jit_" + id()
	f, err := os.Create(name + ".go")
	defer os.Remove(f.Name())
	if err != nil {
		return nil, err
	}

	s := Stencil{
		Name:           name,
		ParameterNames: params,
	}
	err = generate[V](f, s, ast)
	if err != nil {
		return nil, err
	}

	path, err := compile(name + ".go")
	if err != nil {
		return nil, err
	}
	defer os.Remove(path)

	return function[V](s.Name, path)
}

// compile invokes the go compiler to create a shared object / go plugin from
// the go file at the specifed path
func compile(path string) (soPath string, err error) {
	soPath = strings.Replace(path, ".go", ".so", 1)
	cmd := exec.Command("go", "build", "-buildmode=plugin", "-o", soPath, path)
	// TODO: return stderr and stdout in form of an error
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	err = cmd.Run()
	return
}

// function extracts and returns the function with the given name from the
// shared object / go plugin at the given path
func function[V any](name string, path string) (func(V) (V, error), error) {
	p, err := plugin.Open(path)
	if err != nil {
		return nil, err
	}
	symbol, err := p.Lookup(name)
	if err != nil {
		return nil, err
	}

	funct, ok := symbol.(func(any) (any, error))
	if !ok {
		var e func(any) (any, error)
		return nil, fmt.Errorf("Failed to cast symbol of type %T to %T", symbol, e)
	}

	return func(v V) (V, error) {
		var e V
		out, err := funct(v)
		if err != nil {
			return e, err
		}

		return out.(V), nil
	}, nil
}
