package main

import (
	"fmt"
	"log"
	"os"

	"github.com/chzyer/readline"
	"github.com/hneemann/parser2/value"
)

func main() {
	parser := value.New()
	parser.SetOptimizer(nil)
	parser.GetParser().AllowComments()

	if len(os.Args) > 1 {
		fileContent, err := os.ReadFile(os.Args[1])
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

		fmt.Println(result)
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
			fmt.Println("Error:", err)
			continue
		}

		result, err := f.Eval()
		if err != nil {
			fmt.Println("Error:", err)
			continue
		}

		fmt.Println(result)
	}
}
