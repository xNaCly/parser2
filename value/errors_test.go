package value

import (
	"github.com/hneemann/parser2/funcGen"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestErrors(t *testing.T) {
	tests := []struct {
		exp string
		err string
	}{
		{"notFound(a)", "not found: notFound"},
		{"sin(1,2)", "number of args wrong"},
		{"{a:sin(1,2)}", "number of args wrong"},
		{"[].first()", "no items"},
		{"list(0).first()", "no items"},
		{"list(10).multiUse(3)", "needs to be a map"},
		{"list(10).multiUse({a:3})", "contain closures"},
		{"list(10).multiUse({a:(a,b)->a*b})", "one argument"},
		{"list(10).multiUse({a:l->l.map(e->sin(e,1)), b:l->l.reduce((a,b)->a+b)})", "number of args wrong"},
	}

	fg := SetUpParser(New())
	for _, tt := range tests {
		test := tt
		t.Run(test.exp, func(t *testing.T) {
			f, err := fg.Generate(test.exp)
			if err == nil {
				_, err = f(funcGen.NewEmptyStack[Value]())
			}
			if err == nil {
				t.Errorf("expected an error containing '%v'", test.err)
			} else {
				assert.True(t, strings.Contains(err.Error(), test.err), "expected error to containig '%v', got %v", test.err, err.Error())
			}
		})
	}
}