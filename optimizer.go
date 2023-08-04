package parser2

import "fmt"

type optimizer[V any] struct {
	g *FunctionGenerator[V]
}

func NewOptimizer[V any](g *FunctionGenerator[V]) Optimizer {
	return optimizer[V]{g}
}

func (o optimizer[V]) Optimize(ast AST) AST {
	// evaluate constants
	if i, ok := ast.(*Ident); ok {
		if c, ok := o.g.constants[i.Name]; ok {
			return &Const[V]{c, i.Line}
		}
	}
	// evaluate const operations like 1+2
	if oper, ok := ast.(*Operate); ok {
		if operator, ok := o.g.opMap[oper.Operator]; ok {
			if bc, ok := o.isConst(oper.B); ok {
				if operator.IsPure {
					if ac, ok := o.isConst(oper.A); ok {
						return &Const[V]{operator.Impl(ac, bc), oper.Line}
					}
				}
				if operator.IsCommutative {
					if aOp, ok := oper.A.(*Operate); ok && aOp.Operator == oper.Operator {
						if iac, ok := o.isConst(aOp.A); ok {
							return &Operate{
								Operator: oper.Operator,
								A:        &Const[V]{operator.Impl(iac, bc), oper.Line},
								B:        aOp.B,
							}
						}
						if ibc, ok := o.isConst(aOp.B); ok {
							return &Operate{
								Operator: oper.Operator,
								A:        aOp.A,
								B:        &Const[V]{operator.Impl(ibc, bc), oper.Line},
							}
						}
					}
				}
			}
		}
	}

	// evaluate const unary operations like -1
	if oper, ok := ast.(*Unary); ok {
		if operator, ok := o.g.uMap[oper.Operator]; ok {
			if c, ok := o.isConst(oper.Value); ok {
				return &Const[V]{operator.Impl(c), oper.Line}
			}
		}
	}
	// evaluate const if operation
	if ifNode, ok := ast.(*If); ok && o.g.toBool != nil {
		if c, ok := o.isConst(ifNode.Cond); ok {
			if o.g.toBool(c) {
				return ifNode.Then
			} else {
				return ifNode.Else
			}
		}
	}
	// evaluate const list literals like [1,2,3]
	if o.g.listHandler != nil {
		if list, ok := ast.(*ListLiteral); ok {
			if l, ok := o.allConst(list.List); ok {
				return &Const[V]{o.g.listHandler.FromList(l), list.Line}
			}
		}
	}
	// evaluate const map literals like {a:1,b:2}
	if o.g.mapHandler != nil {
		if m, ok := ast.(*MapLiteral); ok {
			cm := map[string]V{}
			for k, e := range m.Map {
				if v, ok := o.isConst(e); ok {
					cm[k] = v
				} else {
					cm = nil
					break
				}
			}
			if cm != nil {
				return &Const[V]{o.g.mapHandler.FromMap(cm), m.Line}
			}
		}
	}
	// evaluate const static function calls like sqrt(2)
	if fc, ok := ast.(*FunctionCall); ok {
		if ident, ok := fc.Func.(*Ident); ok {
			if fu, ok := o.g.staticFunctions[ident.Name]; ok && fu.IsPure {
				if fu.Args >= 0 && fu.Args != len(fc.Args) {
					panic(fmt.Sprintf("number of args wrong in: %v", fc))
				}
				if c, ok := o.allConst(fc.Args); ok {
					return &Const[V]{fu.Func(c), ident.Line}
				}
			}
		}
	}
	return nil
}

func (o optimizer[V]) allConst(asts []AST) ([]V, bool) {
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

func (o optimizer[V]) isConst(ast AST) (V, bool) {
	if n, ok := ast.(*Const[V]); ok {
		return n.Value, true
	}
	var zero V
	return zero, false
}
