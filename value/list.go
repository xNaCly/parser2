package value

import (
	"bytes"
	"fmt"
	"github.com/hneemann/iterator"
	"github.com/hneemann/parser2/funcGen"
	"github.com/hneemann/parser2/listMap"
	"sort"
)

// NewListConvert creates a list containing the given elements if the elements
// do not implement the Value interface. The given function converts the type.
func NewListConvert[I any](conv func(I) Value, items ...I) *List {
	return NewListFromIterable(func() iterator.Iterator[Value] {
		return func(yield func(Value) bool) bool {
			for _, item := range items {
				if !yield(conv(item)) {
					return false
				}
			}
			return true
		}
	})
}

// NewList creates a new list containing the given elements
func NewList(items ...Value) *List {
	return &List{items: items, itemsPresent: true, iterable: func() iterator.Iterator[Value] {
		return func(yield func(Value) bool) bool {
			for _, item := range items {
				if !yield(item) {
					return false
				}
			}
			return true
		}
	}}
}

// NewListFromIterable creates a list based on the given Iterable
func NewListFromIterable(li iterator.Iterable[Value]) *List {
	return &List{iterable: li, itemsPresent: false}
}

// List represents a list of values
type List struct {
	items        []Value
	itemsPresent bool
	iterable     iterator.Iterable[Value]
}

func (l *List) ToMap() (Map, bool) {
	return Map{}, false
}

func (l *List) ToInt() (int, bool) {
	return 0, false
}

func (l *List) ToFloat() (float64, bool) {
	return 0, false
}

func (l *List) String() string {
	var b bytes.Buffer
	b.WriteString("[")
	first := true
	l.iterable()(func(v Value) bool {
		if first {
			first = false
		} else {
			b.WriteString(", ")
		}
		b.WriteString(v.String())
		return true
	})
	b.WriteString("]")
	return b.String()
}

func (l *List) ToBool() (bool, bool) {
	return false, false
}

func (l *List) ToClosure() (funcGen.Function[Value], bool) {
	return funcGen.Function[Value]{}, false
}

func (l *List) ToList() (*List, bool) {
	return l, true
}

func (l *List) Eval() {
	if !l.itemsPresent {
		var it []Value
		l.iterable()(func(value Value) bool {
			it = append(it, value)
			return true
		})
		l.items = it
		l.itemsPresent = true
	}
}

// ToSlice returns the list elements as a slice
func (l *List) ToSlice() []Value {
	l.Eval()
	return l.items[0:len(l.items):len(l.items)]
}

// CopyToSlice creates a slice copy of all elements
func (l *List) CopyToSlice() []Value {
	l.Eval()
	co := make([]Value, len(l.items))
	copy(co, l.items)
	return co
}

// Append creates a new list with a single element appended
// The original list remains unchanged while appending element
// by element is still efficient.
func (l *List) Append(st funcGen.Stack[Value]) *List {
	l.Eval()
	newList := append(l.items, st.Get(1))
	// Guarantee a copy operation the next time append is called on this
	// list, which is only a rare special case, as the new list is usually
	// appended to.
	if len(l.items) != cap(l.items) {
		l.items = l.items[:len(l.items):len(l.items)]
	}
	return NewList(newList...)
}

func (l *List) Size() int {
	l.Eval()
	return len(l.items)
}

func toFunc(name string, st funcGen.Stack[Value], n int, args int) funcGen.Function[Value] {
	if c, ok := st.Get(n).ToClosure(); ok {
		if c.Args == args {
			return c
		} else {
			panic(fmt.Errorf("%d. argument of %s needs to be a closure with %d argoments", n, name, args))
		}
	} else {
		panic(fmt.Errorf("%d. argument of %s needs to be a closure", n, name))
	}
}

func (l *List) Accept(st funcGen.Stack[Value]) *List {
	f := toFunc("accept", st, 1, 1)
	return NewListFromIterable(iterator.FilterAuto[Value](l.iterable, func() func(v Value) bool {
		lst := funcGen.NewEmptyStack[Value]()
		return func(v Value) bool {
			if accept, ok := f.Eval(lst, v).ToBool(); ok {
				return accept
			}
			panic(fmt.Errorf("closure in accept does not return a bool"))
		}
	}))
}

func (l *List) Map(st funcGen.Stack[Value]) *List {
	f := toFunc("map", st, 1, 1)
	return NewListFromIterable(iterator.MapAuto[Value, Value](l.iterable, func() func(i int, v Value) Value {
		lst := funcGen.NewEmptyStack[Value]()
		return func(i int, v Value) Value {
			return f.Eval(lst, v)
		}
	}))
}

type Sortable struct {
	items []Value
	st    funcGen.Stack[Value]
	less  funcGen.Function[Value]
}

func (s Sortable) Len() int {
	return len(s.items)
}

func (s Sortable) Less(i, j int) bool {
	s.st.Push(s.items[i])
	s.st.Push(s.items[j])
	if l, ok := s.less.Func(s.st.CreateFrame(2), nil).ToBool(); ok {
		return l
	} else {
		panic("closure in order needs to return a bool")
	}
}

func (s Sortable) Swap(i, j int) {
	s.items[i], s.items[j] = s.items[j], s.items[i]
}

func (l *List) IndexOf(st funcGen.Stack[Value]) Int {
	v := st.Get(1)
	index := -1
	i := 0
	l.iterable()(func(value Value) bool {
		if Equal(v, value) {
			index = i
			return false
		}
		i++
		return true
	})
	return Int(index)
}

func (l *List) Order(st funcGen.Stack[Value]) *List {
	f := toFunc("order", st, 1, 2)

	items := l.CopyToSlice()

	s := Sortable{
		items: items,
		st:    st,
		less:  f,
	}

	sort.Sort(s)
	return NewList(items...)
}

func (l *List) Reverse() *List {
	items := l.CopyToSlice()
	//reverse items
	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}
	return NewList(items...)
}

func (l *List) Combine(st funcGen.Stack[Value]) *List {
	f := toFunc("combine", st, 1, 2)
	return NewListFromIterable(iterator.Combine[Value, Value](l.iterable, func(a, b Value) Value {
		st.Push(a)
		st.Push(b)
		return f.Func(st.CreateFrame(2), nil)
	}))
}

func (l *List) IIr(st funcGen.Stack[Value]) *List {
	initial := toFunc("iir", st, 1, 1)
	function := toFunc("iir", st, 2, 2)
	return NewListFromIterable(iterator.IirMap[Value, Value](l.iterable,
		func(item Value) Value {
			return initial.Eval(st, item)
		},
		func(item Value, lastItem Value, last Value) Value {
			st.Push(item)
			st.Push(last)
			return function.Func(st.CreateFrame(2), nil)
		}))
}

func (l *List) Visit(st funcGen.Stack[Value]) Value {
	visitor := st.Get(1)
	function := toFunc("visit", st, 2, 2)
	l.iterable()(func(value Value) bool {
		st.Push(visitor)
		st.Push(value)
		visitor = function.Func(st.CreateFrame(2), nil)
		return true
	})
	return visitor
}

func (l *List) Top(st funcGen.Stack[Value]) *List {
	if i, ok := st.Get(1).ToInt(); ok {
		return NewListFromIterable(func() iterator.Iterator[Value] {
			count := i
			return func(yield func(Value) bool) bool {
				return l.iterable()(func(value Value) bool {
					if !yield(value) {
						return false
					}
					count--
					return count > 0
				})
			}
		})
	}
	panic("error in top, no int given")
}

func (l *List) Reduce(st funcGen.Stack[Value]) Value {
	f := toFunc("reduce", st, 1, 2)
	res, ok := iterator.Reduce[Value](l.iterable, func(a, b Value) Value {
		st.Push(a)
		st.Push(b)
		return f.Func(st.CreateFrame(2), nil)
	})
	if ok {
		return res
	}
	panic("error in reduce, no items in list")
}

func (l *List) Replace(st funcGen.Stack[Value]) Value {
	f := toFunc("replace", st, 1, 1)
	return f.Eval(st, l)
}

func (l *List) Number(st funcGen.Stack[Value]) *List {
	f := toFunc("number", st, 1, 2)
	return NewListFromIterable(func() iterator.Iterator[Value] {
		return func(yield func(Value) bool) bool {
			n := Int(0)
			return l.iterable()(func(value Value) bool {
				st.Push(n)
				st.Push(value)
				n++
				return yield(f.Func(st.CreateFrame(2), nil))
			})
		}
	})
}

func (l *List) GroupByString(st funcGen.Stack[Value]) *List {
	keyFunc := toFunc("groupByString", st, 1, 1)
	return GroupBy(l, func(value Value) Value {
		st.Push(value)
		key := keyFunc.Func(st.CreateFrame(1), nil)
		return String(key.String())
	})
}

func (l *List) GroupByInt(st funcGen.Stack[Value]) *List {
	keyFunc := toFunc("groupByInt", st, 1, 1)
	return GroupBy(l, func(value Value) Value {
		st.Push(value)
		key := keyFunc.Func(st.CreateFrame(1), nil)
		if i, ok := key.ToInt(); ok {
			return Int(i)
		} else {
			panic("groupByInt requires an int as key")
		}
	})
}

func GroupBy(list *List, keyFunc func(Value) Value) *List {
	m := make(map[Value]*[]Value)
	list.iterable()(func(value Value) bool {
		key := keyFunc(value)
		if l, ok := m[key]; ok {
			*l = append(*l, value)
		} else {
			ll := []Value{value}
			m[key] = &ll
		}
		return true
	})
	var result []Value
	for k, v := range m {
		entry := listMap.New[Value](2)
		entry.Put("key", k)
		entry.Put("value", NewList(*v...))
		result = append(result, Map{entry})
	}
	return NewList(result...)
}

func (l *List) containsItem(item Value) bool {
	found := false
	l.iterable()(func(value Value) bool {
		if Equal(item, value) {
			found = true
			return false
		}
		return true
	})
	return found
}

func (l *List) containsAllItems(lookForList *List) bool {
	lookFor := lookForList.CopyToSlice()

	if l.itemsPresent && len(l.items) < len(lookFor) {
		return false
	}

	l.iterable()(func(value Value) bool {
		for i, lf := range lookFor {
			if Equal(lf, value) {
				lookFor = append(lookFor[0:i], lookFor[i+1:]...)
				break
			}
		}
		return len(lookFor) > 0
	})
	return len(lookFor) == 0
}

var ListMethods = MethodMap{
	"accept":        MethodAtType(2, func(list *List, stack funcGen.Stack[Value]) Value { return list.Accept(stack) }),
	"map":           MethodAtType(2, func(list *List, stack funcGen.Stack[Value]) Value { return list.Map(stack) }),
	"reduce":        MethodAtType(2, func(list *List, stack funcGen.Stack[Value]) Value { return list.Reduce(stack) }),
	"replace":       MethodAtType(2, func(list *List, stack funcGen.Stack[Value]) Value { return list.Replace(stack) }),
	"combine":       MethodAtType(2, func(list *List, stack funcGen.Stack[Value]) Value { return list.Combine(stack) }),
	"indexOf":       MethodAtType(2, func(list *List, stack funcGen.Stack[Value]) Value { return list.IndexOf(stack) }),
	"groupByString": MethodAtType(2, func(list *List, stack funcGen.Stack[Value]) Value { return list.GroupByString(stack) }),
	"groupByInt":    MethodAtType(2, func(list *List, stack funcGen.Stack[Value]) Value { return list.GroupByInt(stack) }),
	"order":         MethodAtType(2, func(list *List, stack funcGen.Stack[Value]) Value { return list.Order(stack) }),
	"reverse":       MethodAtType(1, func(list *List, stack funcGen.Stack[Value]) Value { return list.Reverse() }),
	"append":        MethodAtType(2, func(list *List, stack funcGen.Stack[Value]) Value { return list.Append(stack) }),
	"iir":           MethodAtType(3, func(list *List, stack funcGen.Stack[Value]) Value { return list.IIr(stack) }),
	"visit":         MethodAtType(3, func(list *List, stack funcGen.Stack[Value]) Value { return list.Visit(stack) }),
	"top":           MethodAtType(2, func(list *List, stack funcGen.Stack[Value]) Value { return list.Top(stack) }),
	"number":        MethodAtType(2, func(list *List, stack funcGen.Stack[Value]) Value { return list.Number(stack) }),
	"size":          MethodAtType(1, func(list *List, stack funcGen.Stack[Value]) Value { return Int(list.Size()) }),
	"string":        MethodAtType(1, func(list *List, stack funcGen.Stack[Value]) Value { return String(list.String()) }),
}

func (l *List) GetMethod(name string) (funcGen.Function[Value], error) {
	return ListMethods.Get(name)
}

func (l *List) Equals(other *List) bool {
	a := l.ToSlice()
	b := other.ToSlice()
	if len(a) != len(b) {
		return false
	}
	for i, aa := range a {
		if !Equal(aa, b[i]) {
			return false
		}
	}
	return true
}

func (l *List) Iterator() iterator.Iterator[Value] {
	return l.iterable()
}
