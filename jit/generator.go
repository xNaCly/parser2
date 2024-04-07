package jit

import (
	"io"
	"text/template"

	"github.com/hneemann/parser2"
)

type Stencil struct {
	Name          string
	ParameterName string
}

// stencil is required due to the fact that the go compiler requires each and
// every type to be defined at compile time
//
// - Type is a stripped down enum taken from value.Value containing the relevant subset of types
//
// - Value is the stripped down interface taken from value.Value containing the
// subset of functions the jit will use at this time
var stencil = `
package main

func {{.Name}}({{.ParameterName}} any) (any, error) {
    return {{.ParameterName}}, nil
}
`

var tmpl = template.Must(template.New("stencil").Parse(stencil))

func generate(w io.Writer, s Stencil, ast parser2.AST) error {
	return tmpl.Execute(w, s)
}
