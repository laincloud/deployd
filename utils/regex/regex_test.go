package regex

import (
	"fmt"
	"testing"
)

func TestMatch(t *testing.T) {
	p := MustCompile("a(.*)c(.*)f")
	m := p.Match("abcdef")
	if m == nil {
		fmt.Println("unmatch!")
	} else {
		fmt.Printf("match:%s\n", m.Group(1))
	}

}

func TestPortsMatch(t *testing.T) {
	p := MustCompile("([0-9]*):([0-9]*)/(tcp|udp)")
	m := p.Match("9501:2132/udp")
	if m == nil {
		fmt.Println("unmatch!")
	} else {
		fmt.Printf("match:%s\n", m.Group(3))
	}

	m = p.Match("9501:2f132/fdp")
	if m == nil {
		fmt.Println("unmatch!")
	} else {
		fmt.Printf("match:%s\n", m.Group(3))
	}

}
