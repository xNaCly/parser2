package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/chzyer/readline"
	"github.com/hneemann/parser2/funcGen"
	"github.com/hneemann/parser2/value"
)

func toUnderlying(v value.Value) (o any) {
	switch t := v.(type) {
	case value.Bool:
		o, _ = t.ToBool()
	case value.Int:
		to, _ := t.ToInt()
		o = float64(to)
	case value.Float:
		o, _ = t.ToFloat()
	case value.String:
		o, _ = t.ToString(funcGen.Stack[value.Value]{})
	case value.Map:
		m := make(map[string]any, t.Size())
		t.Iter(func(key string, v value.Value) bool {
			m[key] = toUnderlying(v)
			return true
		})
		o = m
	default:
		panic(fmt.Sprintf("%T conversion to underlying type not supported by jit", t))
	}
	return
}

func toValue(v any) value.Value {
	switch t := v.(type) {
	case int:
		return value.Int(t)
	case float64:
		return value.Float(t)
	case bool:
		return value.Bool(t)
	case string:
		return value.String(t)
	case map[string]any:
		m := make(value.RealMap, len(t))
		for k, v := range t {
			m[k] = toValue(v)
		}
		return value.NewMap(m)
	default:
		panic(fmt.Sprintf("%T conversion to high level type not supported by jit", t))
	}
}

func main() {
	parser := value.New()
	parser.GetParser().AllowComments()
	jitEnabled := flag.Bool("jit", false, "enable/disable the just in time compiler")
	flag.Parse()
	if *jitEnabled {
		log.Println("[JIT] enabled, starting up")
		parser.SetJit()
		parser.GetJit().UnderlyingToValue = toValue
		parser.GetJit().ValueToUnderlying = toUnderlying
		parser.GetJit().TypeToString = func(v value.Value) string {
			switch v.(type) {
			case value.String:
				return "string"
			case value.Float, value.Int:
				return "float64"
			case value.Bool:
				return "bool"
			case value.Map:
				return "map[string]any"
			default:
				return "any"
			}
		}
	}

	if len(flag.Args()) >= 1 {
		fileContent, err := os.ReadFile(flag.Arg(0))
		start := time.Now()
		f, err := parser.Generate(string(fileContent))
		if err != nil {
			log.Fatalln("Parser error:", err)
			return
		}

		log.Println("Starting eval")
		result, err := f.Eval()
		if err != nil {
			log.Fatalln("Eval error:", err)
			return
		}
		log.Println(result, time.Since(start))
		if *jitEnabled {
			jit := parser.GetJit()
			jit.Cancel()
			<-jit.Ctx.Done()
			log.Println("[JIT] got stop signal, stopped jit")
		}
		return
	}

	rl, err := readline.NewEx(&readline.Config{
		Prompt: "λ ",
	})
	if err != nil {
		panic(err)
	}
	defer rl.Close()

	for {
		input, err := rl.Readline()
		if err != nil {
			break
		}

		if input == "exit" {
			break
		}

		f, err := parser.Generate(input)
		if err != nil {
			log.Println("Error:", err)
			continue
		}

		result, err := f.Eval()
		if err != nil {
			log.Println("Error:", err)
			continue
		}

		fmt.Println(result)
	}
}
