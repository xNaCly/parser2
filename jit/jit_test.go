package jit

import (
	"testing"

	"github.com/hneemann/parser2/value"
	"github.com/stretchr/testify/assert"
)

func TestJIT(t *testing.T) {
	type Test[V comparable] struct {
		input  string
		output V
	}
	tests := []Test[bool]{
		{
			input:  "true",
			output: true,
		},
		{
			input:  "let a = true; a",
			output: true,
		},
	}
	parser := value.New()
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			ast, err := parser.CreateAst(test.input)
			assert.NoError(t, err)
			_, err = Jit[bool](ast)
			assert.NoError(t, err)
			// out, err := f(funcGen.Stack[bool]{}, nil)
			// assert.NoError(t, err)
			// assert.Equal(t, test.output, out)
		})
	}
}
