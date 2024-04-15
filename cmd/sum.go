package main

import (
	"errors"
	"fmt"
	"math"

	"github.com/hneemann/parser2/funcGen"
	"github.com/hneemann/parser2/value"
)

func main() {
	parser := value.New()
	parser.AddStaticFunction("pow", funcGen.Function[value.Value]{
		Args: 2,
		Func: func(st funcGen.Stack[value.Value], clsoureStore []value.Value) (value.Value, error) {
			base, valid := st.Get(0).ToFloat()
			if !valid {
				return nil, errors.New("pow: base is not a number")
			}

			exp, valid := st.Get(1).ToFloat()
			if !valid {
				return nil, errors.New("pow: exponent is not a number")
			}

			return value.Float(math.Pow(base, exp)), nil
		},
	})
	parser.AddSimpleFunction("double", func(val value.Value) value.Value {
		num, _ := val.ToFloat()
		return value.Float(num * 2)
	})

	query, err := parser.Generate("input.accept(i -> i % 2 = 0).map(i -> double(i)).map(n -> pow(n, 4))", "input")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	inputs := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	inputList := value.NewListConvert(func(i int) value.Value { return value.Int(i) }, inputs)
	result, err := query.Eval(inputList)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	fmt.Println(result)
}
