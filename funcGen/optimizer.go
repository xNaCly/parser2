package funcGen

import (
	"fmt"
	"github.com/hneemann/parser2"
	"github.com/hneemann/parser2/listMap"
)

type optimizer[V any] struct {
	g *FunctionGenerator[V]
}

func NewOptimizer[V any](g *FunctionGenerator[V]) parser2.Optimizer {
	return optimizer[V]{g}
}

func (o optimizer[V]) Optimize(ast parser2.AST) (parser2.AST, error) {
	// evaluate const operations like 1+2
	if oper, ok := ast.(*parser2.Operate); ok {
		if operator, ok := o.g.opMap[oper.Operator]; ok {
			if bc, ok := o.isConst(oper.B); ok {
				if operator.IsPure {
					if ac, ok := o.isConst(oper.A); ok {
						co, err := operator.Impl(ac, bc)
						if err != nil {
							return nil, fmt.Errorf("error in const operation %s: %w", operator.Operator, err)
						}
						return &parser2.Const[V]{co, oper.Line}, nil
					}
				}
				if operator.IsCommutative {
					if aOp, ok := oper.A.(*parser2.Operate); ok && aOp.Operator == oper.Operator {
						if iac, ok := o.isConst(aOp.A); ok {
							co, err := operator.Impl(iac, bc)
							if err != nil {
								return nil, fmt.Errorf("error in const operation %s: %w", operator.Operator, err)
							}
							return &parser2.Operate{
								Operator: oper.Operator,
								A:        &parser2.Const[V]{co, oper.Line},
								B:        aOp.B,
							}, nil
						}
						if ibc, ok := o.isConst(aOp.B); ok {
							co, err := operator.Impl(ibc, bc)
							if err != nil {
								return nil, fmt.Errorf("error in const operation %s: %w", operator.Operator, err)
							}
							return &parser2.Operate{
								Operator: oper.Operator,
								A:        aOp.A,
								B:        &parser2.Const[V]{co, oper.Line},
							}, nil
						}
					}
				}
			}
		}
	}

	// evaluate const unary operations like -1
	if oper, ok := ast.(*parser2.Unary); ok {
		if operator, ok := o.g.uMap[oper.Operator]; ok {
			if c, ok := o.isConst(oper.Value); ok {
				co, err := operator.Impl(c)
				if err != nil {
					return nil, fmt.Errorf("error in const unary operation %s: %w", operator.Operator, err)
				}
				return &parser2.Const[V]{co, oper.Line}, nil
			}
		}
	}
	// evaluate const if operation
	if ifNode, ok := ast.(*parser2.If); ok && o.g.toBool != nil {
		if c, ok := o.isConst(ifNode.Cond); ok {
			if cond, ok := o.g.toBool(c); ok {
				if cond {
					return ifNode.Then, nil
				} else {
					return ifNode.Else, nil
				}
			} else {
				return nil, fmt.Errorf("if condition is not a bool")
			}
		}
	}
	// evaluate const list literals like [1,2,3]
	if o.g.listHandler != nil {
		if list, ok := ast.(*parser2.ListLiteral); ok {
			if l, ok := o.allConst(list.List); ok {
				return &parser2.Const[V]{o.g.listHandler.FromList(l), list.Line}, nil
			}
		}
	}
	// evaluate const map literals like {a:1,b:2}
	if o.g.mapHandler != nil {
		if m, ok := ast.(*parser2.MapLiteral); ok {
			cm := listMap.New[V](len(m.Map))
			if m.Map.Iter(func(key string, value parser2.AST) bool {
				if v, ok := o.isConst(value); ok {
					cm = cm.Append(key, v)
				} else {
					cm = nil
					return false
				}
				return true
			}) {
				return &parser2.Const[V]{o.g.mapHandler.FromMap(cm), m.Line}, nil
			}
		}
	}
	// evaluate const static function calls like sqrt(2)
	if fc, ok := ast.(*parser2.FunctionCall); ok {
		if ident, ok := fc.Func.(*parser2.Ident); ok {
			if fu, ok := o.g.staticFunctions[ident.Name]; ok && fu.IsPure {
				if fu.Args >= 0 && fu.Args != len(fc.Args) {
					return nil, fmt.Errorf("number of args wrong in: %v", fc)
				}
				if c, ok := o.allConst(fc.Args); ok {
					v, err := fu.Func(NewStack[V](c...), nil)
					if err != nil {
						return nil, fmt.Errorf("error in const function call %s: %w", ident, err)
					}
					return &parser2.Const[V]{v, ident.Line}, nil
				}
			}
		}
	}
	return nil, nil
}

func (o optimizer[V]) allConst(asts []parser2.AST) ([]V, bool) {
	con := make([]V, len(asts))
	for i, ast := range asts {
		if c, ok := o.isConst(ast); ok {
			con[i] = c
		} else {
			return nil, false
		}
	}
	return con, true
}

func (o optimizer[V]) isConst(ast parser2.AST) (V, bool) {
	if n, ok := ast.(*parser2.Const[V]); ok {
		return n.Value, true
	}
	var zero V
	return zero, false
}
