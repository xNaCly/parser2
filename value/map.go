package value

import (
	"bytes"
	"fmt"
	"github.com/hneemann/iterator"
	"github.com/hneemann/parser2/funcGen"
	"github.com/hneemann/parser2/listMap"
)

type MapStorage interface {
	Get(key string) (Value, bool)
	Iter(yield func(key string, v Value) bool) bool
	Size() int
}

type creator struct {
	m listMap.ListMap[Value]
}

// MapCreator is used to create a new list based map
func MapCreator(size int) creator {
	return creator{m: make(listMap.ListMap[Value], 0, size)}
}

// Put adds an entry to the map
func (c creator) Put(key string, v Value) creator {
	c.m.Put(key, v)
	return c
}

// Map creates the map
func (c creator) Map() Map {
	return Map{m: c.m}
}

type RealMap map[string]Value

func (s RealMap) Get(key string) (Value, bool) {
	v, ok := s[key]
	return v, ok
}

func (s RealMap) Iter(yield func(key string, v Value) bool) bool {
	for k, v := range s {
		if !yield(k, v) {
			return false
		}
	}
	return true
}

func (s RealMap) Size() int {
	return len(s)
}

type Map struct {
	m MapStorage
}

func (v Map) Iter(yield func(key string, v Value) bool) bool {
	return v.m.Iter(yield)
}

func (v Map) ToList() (*List, bool) {
	return nil, false
}

func (v Map) ToInt() (int, bool) {
	return 0, false
}

func (v Map) ToFloat() (float64, bool) {
	return 0, false
}

func (v Map) String() string {
	var b bytes.Buffer
	b.WriteString("{")
	first := true
	v.m.Iter(func(key string, v Value) bool {
		if first {
			first = false
		} else {
			b.WriteString(", ")
		}
		b.WriteString(key)
		b.WriteString(":")
		b.WriteString(v.String())
		return true
	})
	b.WriteString("}")
	return b.String()
}

func (v Map) ToBool() (bool, bool) {
	return false, false
}

func (v Map) ToClosure() (funcGen.Function[Value], bool) {
	return funcGen.Function[Value]{}, false
}

func (v Map) ToMap() (Map, bool) {
	return v, true
}
func (v Map) Size() int {
	return v.m.Size()
}

func (v Map) Equals(other Map) bool {
	if v.Size() != other.Size() {
		return false
	}
	equal := true
	v.m.Iter(func(key string, v Value) bool {
		if o, ok := other.Get(key); ok {
			if !Equal(o, v) {
				equal = false
				return false
			}
		} else {
			equal = false
			return false
		}
		return true
	})
	return equal
}

func (v Map) Accept(st funcGen.Stack[Value]) Map {
	f := toFunc("accept", st, 1, 2)
	newMap := listMap.New[Value](v.m.Size())
	v.m.Iter(func(key string, v Value) bool {
		st.Push(String(key))
		st.Push(v)
		if cond, ok := f.Func(st.CreateFrame(2), nil).ToBool(); ok {
			if cond {
				newMap.Put(key, v)
			}
		} else {
			panic(fmt.Errorf("closure in accept does not return a bool"))
		}
		return true
	})
	return Map{m: newMap}
}

func (v Map) Map(st funcGen.Stack[Value]) Map {
	f := toFunc("map", st, 1, 2)
	newMap := listMap.New[Value](v.m.Size())
	v.m.Iter(func(key string, v Value) bool {
		st.Push(String(key))
		st.Push(v)
		newMap.Put(key, f.Func(st.CreateFrame(2), nil))
		return true
	})
	return Map{m: newMap}
}

func (v Map) Replace(st funcGen.Stack[Value]) Value {
	f := toFunc("replace", st, 1, 1)
	return f.Eval(st, v)
}

func (v Map) List() *List {
	return NewListFromIterable(func() iterator.Iterator[Value] {
		return func(yield func(Value) bool) bool {
			v.m.Iter(func(key string, v Value) bool {
				m := listMap.New[Value](2)
				m.Put("key", String(key))
				m.Put("value", v)
				return yield(Map{m})
			})
			return true
		}
	})
}

func (v Map) Get(key string) (Value, bool) {
	return v.m.Get(key)
}

func (v Map) IsAvail(stack funcGen.Stack[Value]) Value {
	if key, ok := stack.Get(1).(String); ok {
		_, ok := v.m.Get(string(key))
		return Bool(ok)
	}
	panic("isAvail requires a string as argument")
}

func (v Map) GetM(stack funcGen.Stack[Value]) Value {
	if key, ok := stack.Get(1).(String); ok {
		if v, ok := v.m.Get(string(key)); ok {
			return v
		} else {
			panic(fmt.Errorf("key %v not found in map", key))
		}
	}
	panic("get requires a string as argument")
}

type AppendMap struct {
	key    string
	value  Value
	parent MapStorage
}

func (a AppendMap) Get(key string) (Value, bool) {
	if key == a.key {
		return a.value, true
	} else {
		return a.parent.Get(key)
	}
}

func (a AppendMap) Iter(yield func(key string, v Value) bool) bool {
	if !yield(a.key, a.value) {
		return false
	} else {
		return a.parent.Iter(yield)
	}
}

func (a AppendMap) Size() int {
	return a.parent.Size() + 1
}

func (v Map) PutM(stack funcGen.Stack[Value]) Map {
	if key, ok := stack.Get(1).(String); ok {
		val := stack.Get(2)
		return Map{AppendMap{key: string(key), value: val, parent: v.m}}
	}
	panic("get requires a string as argument")
}

var MapMethods = MethodMap{
	"accept":  methodAtType(2, func(m Map, stack funcGen.Stack[Value]) Value { return m.Accept(stack) }),
	"map":     methodAtType(2, func(m Map, stack funcGen.Stack[Value]) Value { return m.Map(stack) }),
	"replace": methodAtType(2, func(m Map, stack funcGen.Stack[Value]) Value { return m.Replace(stack) }),
	"list":    methodAtType(1, func(m Map, stack funcGen.Stack[Value]) Value { return m.List() }),
	"size":    methodAtType(1, func(m Map, stack funcGen.Stack[Value]) Value { return Int(m.Size()) }),
	"isAvail": methodAtType(2, func(m Map, stack funcGen.Stack[Value]) Value { return m.IsAvail(stack) }),
	"get":     methodAtType(2, func(m Map, stack funcGen.Stack[Value]) Value { return m.GetM(stack) }),
	"put":     methodAtType(3, func(m Map, stack funcGen.Stack[Value]) Value { return m.PutM(stack) }),
}

func (v Map) GetMethod(name string) (funcGen.Function[Value], error) {
	return MapMethods.Get(name)
}
