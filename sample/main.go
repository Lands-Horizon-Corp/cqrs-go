package main

import (
	"context"
	"fmt"
	"time"
)

// GET api/v1/bank (5s)
func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	fmt.Println(1)
	time.Sleep(5 * time.Second)
	fmt.Println(2)
	fmt.Println(ctx)
	cancel()
	time.Sleep(3 * time.Second)
}
