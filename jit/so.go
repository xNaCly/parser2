package jit

import (
	"fmt"
	"plugin"

	"github.com/hneemann/parser2/value"
)

type Function func(value.Value) (value.Value, error)

// function extracts and returns the function with the given name from the
// shared object / go plugin at the given path
func function(name string, path string) (Function, error) {
	p, err := plugin.Open(path)
	if err != nil {
		return nil, err
	}
	symbol, err := p.Lookup(name)
	if err != nil {
		return nil, err
	}
	funct, ok := symbol.(func(value.Value) (value.Value, error))
	if !ok {
		var e Function
		return nil, fmt.Errorf("Failed to cast symbol of type %T to %T", symbol, e)
	}
	return funct, nil
}
