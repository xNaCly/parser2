package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/chzyer/readline"
	"github.com/hneemann/parser2/value"
)

func main() {
	parser := value.New()
	parser.GetParser().AllowComments()
	jitEnabled := flag.Bool("jit", false, "enable/disable the just in time compiler")
	flag.Parse()
	if *jitEnabled {
		parser.SetJit()
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
