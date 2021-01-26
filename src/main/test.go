package main

var a string
var done bool
var c chan int = make(chan int)

func setup() {
	a = "hello, world"
	c <- 0
}

func main() {
	go setup()
	<-c
	print(a)
}
