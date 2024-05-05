package funcGen

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"plugin"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/hneemann/parser2"
)

// TODO:
// - named functions are currently not being compiled

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
	Queue  chan *Function[V]
	Ctx    context.Context
	Cancel context.CancelFunc
	// counter is used for compiling closures and keeping track of them in shared objects
	counter uint64
	// TypeToString is used to convert the given arguments type to a string
	// representation the jit compiler uses to assert the function parameters
	// type
	TypeToString func(V) string
	// ValueToUnderlying converts the argument to its underlying go type
	ValueToUnderlying func(V) any
	// UnderlyingToValue converts the argument to the generic type
	UnderlyingToValue func(any) V
}

// Invokes the code generation, traverses the abstract syntax tree, calls
// the go compiler, opens the compiled plugin and returns the generated and
// compiled function
func (j *Jit[V]) Compile(fun *Function[V]) error {
	start := time.Now()
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
	b.WriteString(`package main;`)
	// logic for naming closures
	if len(fun.Name) == 0 {
		fun.Name = string([]byte{'c', byte(j.counter + '0')})
		j.counter++
	}
	c := fun.Ast.(*parser2.ClosureLiteral)
	c.Name = fun.Name
	log.Printf("[JIT] attempting to compile %q\n", c.Name)
	err = j.generateFunction(&b, c, fun.MetaData)
	if err != nil {
		return err
	}
	log.Println("[JIT] compiled to:", b.String())
	if _, err := b.WriteTo(f); err != nil {
		return err
	}
	path, err := invokeCompiler(f.Name())
	if err != nil {
		return err
	}
	defer os.Remove(path)
	funct, err := j.function("JIT_"+fun.Name, path)
	if err != nil {
		log.Printf("[JIT] failed to compile %s: %s\n", fun.Name, err)
		return err
	}
	fun.JitFunc = funct
	log.Printf("[JIT] compiling %q took %s\n", c.Name, time.Since(start))
	return nil
}

// generateFunction generates the go code for a given closure recursively
func (j *Jit[V]) generateFunction(b *bytes.Buffer, fun *parser2.ClosureLiteral, m *MetaData) error {
	b.WriteString("func ")
	b.WriteString("JIT_")
	b.WriteString(fun.Name)
	b.WriteString("(args ...any) (any, error) { ")
	for i, arg := range m.Parameters {
		b.WriteString(arg.Name)
		b.WriteString(" := ")
		b.WriteString("args[")
		if i > 9 {
			return errors.New("More than 9 arguments not supported for compiled functions")
		}
		b.WriteRune(rune(i + 48))
		b.WriteString("].(")
		b.WriteString(arg.Type)
		b.WriteString(");")
	}
	b.WriteString("return ")
	err := j.codegen(b, fun.Func)
	if err != nil {
		return err
	}
	b.WriteString(", nil}")
	return err
}

func (j *Jit[V]) codeGenWithoutAstTypes(b *bytes.Buffer, a any) error {
	switch t := a.(type) {
	case *parser2.Const[V]:
		err := j.constantWriter(b, j.ValueToUnderlying(t.Value))
		if err != nil {
			return err
		}
	case *parser2.Let:
		b.WriteString(t.Name)
		b.WriteString(":=")
		err := j.codeGenWithoutAstTypes(b, t.Value)
		if err != nil {
			return err
		}
	case *parser2.MapLiteral:
		err := j.constantWriter(b, t.Map.ToNative())
		if err != nil {
			return err
		}
	case *parser2.Ident:
		b.WriteString(t.Name)
	case *parser2.MapAccess:
		j.codegen(b, t.MapValue)
		b.WriteString("[\"")
		b.WriteString(t.Key)
		b.WriteString("\"]")
	case *parser2.Operate:
		err := j.codeGenWithoutAstTypes(b, t.A)
		if err != nil {
			return err
		}
		op := t.Operator
		switch op {
		case "=":
			op = "=="
		case "&":
			op = "&&"
		case "|":
			op = "||"
		}
		b.WriteString(op)
		err = j.codeGenWithoutAstTypes(b, t.B)
		if err != nil {
			return err
		}
	case *parser2.Unary:
		b.WriteString(t.Operator)
		err := j.codeGenWithoutAstTypes(b, t.Value)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("Codegen: Expression %T not yet supported by jit", t)
	}
	return nil
}

func (j *Jit[V]) constantWriter(b *bytes.Buffer, a any) error {
	switch t := a.(type) {
	case int:
		b.WriteString(strconv.FormatInt(int64(t), 10))
	case float64:
		b.WriteString(strconv.FormatFloat(t, 'E', -1, 64))
	case string:
		b.WriteRune('"')
		b.WriteString(t)
		b.WriteRune('"')

		// TODO: test this
		// case []any:
		// 	b.WriteString("[]any{")
		// 	for _, v := range t {
		// 		err := j.writer(b, v)
		// 		if err != nil {
		// 			return err
		// 		}
		// 		b.WriteRune(',')
		// 	}
		// 	b.WriteRune('}')
	case map[string]any:
		b.WriteString("map[string]any{")
		for k, v := range t {
			b.WriteRune('"')
			b.WriteString(k)
			b.WriteString("\":")
			err := j.codeGenWithoutAstTypes(b, v)
			if err != nil {
				return err
			}
			b.WriteRune(',')
		}
		b.WriteRune('}')
	default:
		return fmt.Errorf("Codegen: Expression %T not yet supported by constantWriter jit component", t)
	}
	return nil
}

func (j *Jit[V]) codegen(b *bytes.Buffer, ast parser2.AST) error {
	return j.codeGenWithoutAstTypes(b, ast)
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
func (j *Jit[V]) function(name string, path string) (func(...any) (V, error), error) {
	p, err := plugin.Open(path)
	if err != nil {
		return nil, err
	}
	symbol, err := p.Lookup(name)
	if err != nil {
		return nil, err
	}

	funct, ok := symbol.(func(...any) (any, error))
	if !ok {
		var e func(...any) (any, error)
		return nil, fmt.Errorf("Failed to cast symbol of type %T to %T", symbol, e)
	}
	return func(a ...any) (V, error) {
		out, err := funct(a...)
		if err != nil {
			var e V
			return e, err
		}
		return j.UnderlyingToValue(out), nil
	}, nil
}
