package jit

import (
	"testing"

	"github.com/hneemann/parser2/value"
	"github.com/stretchr/testify/assert"
)

func TestJIT(t *testing.T) {
	type Test[V comparable] struct {
		input      string
		inputValue value.Value
		output     value.Value
	}
	tests := []Test[bool]{
		{
			input:      "func a(arg) arg;a(true)",
			inputValue: value.Bool(true),
			output:     value.Bool(true),
		},
	}
	parser := value.New()
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			ast, err := parser.CreateAst(test.input)
			assert.NoError(t, err)
			f, err := Jit(ast)
			assert.NoError(t, err)
			out, err := f(test.inputValue)
			assert.NoError(t, err)
			assert.Equal(t, test.output, out)
		})
	}
}
