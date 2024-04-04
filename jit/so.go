package jit

import "github.com/hneemann/parser2/funcGen"

// function extracts and returns the function with the given name from the
// shared object / go plugin at the given path
func function[V any](name string, path string) (func(stack funcGen.Stack[V], closureStore []V) (V, error), error) {
	// TODO: function lookup in go plugin
	return nil, nil
}
