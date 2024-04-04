package jit

import (
	"fmt"
	"io"
	"strings"
	"text/template"
)

// Stencil represents a singular function taking an argument of type
// ArgumentType, returning a value of type ReturnType and being named
// FunctionName
type Stencil[ParameterType any, ReturnType any] struct {
	Name                string
	Parameter           ParameterType
	ParameterName       string
	Return              ReturnType
	ReturnTypeString    string // string representation of Return
	ParameterTypeString string // string representation of Argument
}

// stencil is required due to the fact that the go compiler requires each and
// every type to be defined at compile time
var stencil = `
package main

func {{.Name}}({{.ParameterName}} {{.ParameterTypeString}}) ({{.ReturnTypeString}}, error) {
    var e {{.ReturnTypeString}}
    return e, nil
}
`

var tmpl = template.Must(template.New("stencil").Parse(stencil))

func generate[ArgumentType any, ReturnType any](w io.Writer, s Stencil[ArgumentType, ReturnType]) error {
	if s.ParameterTypeString == "" || s.ReturnTypeString == "" {
		types := strings.Split(fmt.Sprintf("%T;%T", s.Parameter, s.Return), ";")
		s.ParameterTypeString = types[0]
		s.ReturnTypeString = types[1]
	}
	return tmpl.Execute(w, s)
}
