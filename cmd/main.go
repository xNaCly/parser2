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

func main() {
	parser := value.New()
	parser.GetParser().AllowComments()
	jitEnabled := flag.Bool("jit", false, "enable/disable the just in time compiler")
	flag.Parse()
	if *jitEnabled {
		log.Println("[JIT] enabled, starting up")
		parser.SetJit()
		parser.GetJit().UnderlyingToValue = func(v any) value.Value {
			switch t := v.(type) {
			case int:
				return value.Int(t)
			case float64:
				return value.Float(t)
			case bool:
				return value.Bool(t)
			case string:
				return value.String(t)
			default:
				panic(fmt.Sprintf("%T conversion to value not supported by jit", t))
			}
		}
		parser.GetJit().ValueToUnderlying = func(v value.Value) any {
			switch t := v.(type) {
			case value.Bool:
				o, _ := t.ToBool()
				return o
			case value.Int:
				o, _ := t.ToInt()
				return o
			case value.Float:
				o, _ := t.ToFloat()
				return o
			case value.String:
				o, _ := t.ToString(funcGen.Stack[value.Value]{})
				return o
			default:
				panic(fmt.Sprintf("%T conversion to underlying type not supported by jit", t))
			}
		}
		parser.GetJit().TypeToString = func(v value.Value) string {
			switch v.(type) {
			case value.String:
				return "string"
			case value.Int:
				return "int"
			case value.Float:
				return "float64"
			case value.Bool:
				return "bool"
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

		result, err := f.Eval()
		if err != nil {
			log.Fatalln("Eval error:", err)
			return
		}
		if *jitEnabled {
			jit := parser.GetJit()
			jit.Cancel()
			<-jit.Ctx.Done()
			log.Println("[JIT] got stop signal, stopped jit")
		}
		log.Println(result, time.Since(start))
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
