package main

import (
	"fmt"
	"log"

	"github.com/prabhatdotdev/weave/codec"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func main() {
	msg, err := codec.MarshalMessage(codec.Protobuf, wrapperspb.String("hello"))
	if err != nil {
		log.Fatal(err)
	}

	var decoded wrapperspb.StringValue
	if err := codec.UnmarshalMessage(codec.Protobuf, msg, &decoded); err != nil {
		log.Fatal(err)
	}
	fmt.Println(decoded.Value)
}
