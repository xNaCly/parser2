package value

import (
	"strings"
	"testing"

	"github.com/hneemann/parser2/funcGen"
	"github.com/stretchr/testify/assert"
)

func BenchmarkJit(b *testing.B) {
	inputs := []struct {
		name  string
		input string
	}{
		{
			name:  "anonymous functions",
			input: "func b(a) a; list(%iterations%).map(b).size()",
		},
		{
			name:  "anonymous functions v1",
			input: "list(%iterations%).map(e -> e).size()",
		},
	}
	iterations := []string{"10_000", "100_000", "1_000_000", "3_000_000"}
	for _, input := range inputs {
		for _, iter := range iterations {
			input.input = strings.Replace(input.input, "%iterations%", iter, 1)
			b.Run(input.name+"_NOJIT_"+iter, func(b *testing.B) {
				parser := New()
				parser.GetParser().AllowComments()
				f, err := parser.Generate(input.input)
				assert.NoError(b, err)
				_, err = f.Eval()
				assert.NoError(b, err)
			})
		}
		for _, iter := range iterations {
			input.input = strings.Replace(input.input, "%iterations%", iter, 1)
			b.Run(input.name+"_JIT_"+iter+"_JIT_CONSTANT=1_000", func(b *testing.B) {
				parser := New()
				parser.GetParser().AllowComments()
				parser.SetJit()
				defer parser.GetJit().Cancel()
				f, err := parser.Generate(input.input)
				assert.NoError(b, err)
				_, err = f.Eval()
				assert.NoError(b, err)
			})
		}
		for _, iter := range iterations {
			input.input = strings.Replace(input.input, "%iterations%", iter, 1)
			b.Run(input.name+"_JIT_"+iter+"_JIT_CONSTANT=10_000", func(b *testing.B) {
				funcGen.JIT_CONSTANT = 10_000
				parser := New()
				parser.GetParser().AllowComments()
				parser.SetJit()
				defer parser.GetJit().Cancel()
				f, err := parser.Generate(input.input)
				assert.NoError(b, err)
				_, err = f.Eval()
				assert.NoError(b, err)
			})
		}
		for _, iter := range iterations {
			input.input = strings.Replace(input.input, "%iterations%", iter, 1)
			b.Run(input.name+"_JIT_"+iter+"_JIT_CONSTANT=100_000", func(b *testing.B) {
				funcGen.JIT_CONSTANT = 100_000
				parser := New()
				parser.GetParser().AllowComments()
				parser.SetJit()
				defer parser.GetJit().Cancel()
				f, err := parser.Generate(input.input)
				assert.NoError(b, err)
				_, err = f.Eval()
				assert.NoError(b, err)
			})
		}
	}
}
