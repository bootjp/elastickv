package main

import (
	"log"

	"github.com/bootjp/elastickv/cmd"
)

func main() {
	if err := cmd.Run(); err != nil {
		log.Fatal(err)
	}
}
