package jit

import (
	"bytes"
	"fmt"
	"io"

	"github.com/hneemann/parser2"
)

type Stencil struct {
	Name           string
	ParameterNames []string
}

func generate[V any](w io.Writer, s Stencil, ast parser2.AST) error {
	buf := bytes.Buffer{}
	buf.WriteString("package main;func ")
	buf.WriteString(s.Name)
	buf.WriteRune('(')
	for i, arg := range s.ParameterNames {
		buf.WriteString(arg)
		buf.WriteString(" any")
		if i+1 != len(s.ParameterNames) {
			buf.WriteRune(',')
		}
	}
	buf.WriteString(") any {")
	buf.WriteString("return ")
	err := codegen[V](&buf, ast)
	if err != nil {
		return err
	}
	buf.WriteString("}")
	fmt.Println("[JIT] output:", buf.String())
	_, err = buf.WriteTo(w)
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
