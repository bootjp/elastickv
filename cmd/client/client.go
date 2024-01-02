package main

import (
	"log"

	_ "github.com/Jille/grpc-multi-resolver"
	"github.com/bootjp/elastickv/cmd"
	_ "google.golang.org/grpc/health"
)

func main() {
	if err := cmd.Run(); err != nil {
		log.Fatal(err)
	}
}
