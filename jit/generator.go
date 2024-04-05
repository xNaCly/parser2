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
var stencil = `
package main

import (
	"github.com/hneemann/parser2/value"
)

func {{.Name}}({{.ParameterName}} value.Value) (value.Value, error) {
    return value.Bool(true), nil
}
`

var tmpl = template.Must(template.New("stencil").Parse(stencil))

func generate(w io.Writer, s Stencil, ast parser2.AST) error {
	return tmpl.Execute(w, s)
}
