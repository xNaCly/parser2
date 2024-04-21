package funcGen

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"plugin"
	"runtime"
	"strings"

	"github.com/hneemann/parser2"
)

// TODO:
// - accept multiple arguments for closures
// - metatracing for closure argument types
// - type assertions in generated code, see: https://github.com/xNaCly/Sophia/pull/9

// Jit implements just in time compilation of expressions, this requires a not
// to neglect start up time and thus should only be invoked once the callee is
// sure to outperform the interpreted expression using the compiled function.
// Jit works by assuming perfect code and state, if the compilation fails due
// to a compilation error the Jit bails out of execution.
//
// The Jits inner workings are inspired and descendant to the following resources:
//
// - the sophia lang jit compiler: https://github.com/xNaCly/Sophia/pull/9
//
// - the paper documenting the research on this jit: https://github.com/xNaCly/treewalk-vs-jit-with-go-plugins
type Jit[V any] struct {
	// Queue accepts Function pointers the jit compiler should compile in the future
	Queue  []*Function[V]
	ctx    context.Context
	cancel context.CancelFunc
	// counter is used for compiling closures and keeping track of them in shared objects
	counter uint64
}

// Invokes the code generation, traverses the abstract syntax tree, calls
// the go compiler, opens the compiled plugin and returns the generated and
// compiled function
func (j *Jit[V]) Compile() error {
	if runtime.GOOS == "windows" {
		return fmt.Errorf(`
The go plugin api is not supported on windows, just in time compilation is therefore not available.
See: https://pkg.go.dev/plugin#hdr-Warnings (%w)`, errors.ErrUnsupported)
	}

	f, err := os.CreateTemp("", "JIT_*.go")
	defer os.Remove(f.Name())
	if err != nil {
		return err
	}
	b := bytes.Buffer{}
	b.WriteString("package main;")
	for _, fun := range j.Queue {
		// logic for naming closures
		if len(fun.Name) == 0 {
			fun.Name = string([]byte{'c', byte(j.counter + '0')})
			j.counter++
			fmt.Printf("[JIT] Reached %d calls to function %q, adding to compilation queue\n", JIT_CONSTANT, fun.Name)
		}
		c := fun.Ast.(*parser2.ClosureLiteral)
		c.Name = fun.Name
		err = generateFunction[V](&b, c)
		if err != nil {
			return err
		}
	}
	fmt.Println("[JIT] output:", b.String())
	if _, err := b.WriteTo(f); err != nil {
		return err
	}
	path, err := invokeCompiler(f.Name())
	if err != nil {
		return err
	}
	defer os.Remove(path)
	for i, fun := range j.Queue {
		j.Queue[i] = j.Queue[len(j.Queue)-1]
		j.Queue = j.Queue[:len(j.Queue)-1]
		funct, err := function[V]("JIT_"+fun.Name, path)
		if err != nil {
			fmt.Printf("[JIT] failed to compile %s: %s\n", fun.Name, err)
			continue
		}
		fun.JitFunc = func(stack Stack[V], closureStore []V) (V, error) {
			out, err := funct(stack.Get(0))
			var e V
			if err != nil {
				return e, err
			}
			return out, nil
		}
	}
	return nil
}

// generateFunction generates the go code for a given closure recursively
func generateFunction[V any](b *bytes.Buffer, fun *parser2.ClosureLiteral) error {
	b.WriteString("func ")
	b.WriteString("JIT_")
	b.WriteString(fun.Name)
	b.WriteRune('(')
	for i, arg := range fun.Names {
		b.WriteString(arg)
		b.WriteString(" any")
		if i+1 != len(fun.Names) {
			b.WriteRune(',')
		}
	}
	b.WriteString(") any {")
	b.WriteString("return ")
	err := codegen[V](b, fun.Func)
	if err != nil {
		return err
	}
	b.WriteString("}")
	return err
}

func codegen[V any](b *bytes.Buffer, ast parser2.AST) error {
	switch t := ast.(type) {
	case *parser2.Const[V]:
		b.WriteString(t.String())
	case *parser2.Ident:
		b.WriteString(t.Name)
	case *parser2.Operate:
		err := codegen[V](b, t.A)
		if err != nil {
			return err
		}
		b.WriteString(t.Operator)
		err = codegen[V](b, t.B)
		if err != nil {
			return err
		}
	case *parser2.Unary:
		b.WriteString(t.Operator)
		err := codegen[V](b, t.Value)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("Codegen: Expression %T not yet supported by jit: %q", t, t.String())
	}
	return nil
}

// invokeCompiler invokes the go compiler to create a shared object / go plugin from
// the go file at the specifed path
func invokeCompiler(path string) (soPath string, err error) {
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

	funct, ok := symbol.(func(any) any)
	if !ok {
		var e func(any) (any, error)
		return nil, fmt.Errorf("Failed to cast symbol of type %T to %T", symbol, e)
	}

	return func(v V) (V, error) {
		out := funct(v)
		return out.(V), nil
	}, nil
}
