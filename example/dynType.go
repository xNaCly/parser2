package example

import (
	"fmt"
	"github.com/hneemann/parser2/funcGen"
	"github.com/hneemann/parser2/value"
	"math"
)

type ErrValue struct {
	val float64
	err float64
}

func (e ErrValue) ToList() ([]value.Value, bool) {
	return nil, false
}

func (e ErrValue) ToMap() (value.MapImplementation[value.Value], bool) {
	return nil, false
}

func (e ErrValue) ToInt() (int, bool) {
	return int(e.val), true
}

func (e ErrValue) ToFloat() (float64, bool) {
	return e.val, true
}

func (e ErrValue) ToString() (string, bool) {
	return "", false
}

func (e ErrValue) ToBool() (bool, bool) {
	return false, false
}

func (e ErrValue) ToClosure() (funcGen.Function[value.Value], bool) {
	return funcGen.Function[value.Value]{}, false
}

func (e ErrValue) GetMethod(name string) (funcGen.Function[value.Value], bool) {
	return funcGen.Function[value.Value]{}, false
}

var DynType = value.New().
	AddConstant("pi", value.Float(math.Pi)).
	AddConstant("true", value.Bool(true)).
	AddConstant("false", value.Bool(false)).
	AddStaticFunction("sprintf", funcGen.Function[value.Value]{
		Func:   sprintf,
		Args:   -1,
		IsPure: true,
	})

func sprintf(st funcGen.Stack[value.Value], cs []value.Value) value.Value {
	switch st.Size() {
	case 0:
		return value.String("")
	case 1:
		return value.String(fmt.Sprint(st.Get(0)))
	default:
		if s, ok := st.Get(0).(value.String); ok {
			values := make([]any, st.Size()-1)
			for i := 1; i < st.Size(); i++ {
				values[i-1] = st.Get(i)
			}
			return value.String(fmt.Sprintf(string(s), values...))
		} else {
			panic("sprintf requires string as first argument")
		}
	}
}
