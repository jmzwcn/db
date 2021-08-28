package db

import (
	"fmt"
	"regexp"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestCast(t *testing.T) {
	fmt.Println("beign")
	var msg proto.Message
	//msg = &pb.Account{Id: "abc"}
	a := msg.ProtoReflect().New()
	aa := proto.Clone(msg)
	fmt.Println(msg.(proto.Message), a.(protoreflect.Message), aa)
	t.Error("done")
}

func TestPrimaryKey(t *testing.T) {
	//checkTable("test")
	//DB.Exec("drop table test")
}

func TestJsonEscape(t *testing.T) {
	goods := Goods{Id: "iii"}
	goods.Desc = "~!@#$%^&*()_+{}|:\";',./<>?'"
	//fmt.Println(ToJSON(goods))
	// if err := Insert("test1", goods); err != nil {
	// 	t.Error(err)
	// }
	var g1 Goods
	// if err := GetById("test1", "iii", &g1); err != nil {
	// 	t.Error(err)
	// }
	fmt.Println(g1.Desc)
	//DB.Exec("drop table test1")
	t.Error("done")
}

type Goods struct {
	Id   string `json:"id,omitempty"`
	Name string
	Desc string
}

func TestRegexp(t *testing.T) {
	matched, err := regexp.MatchString("s.*a.*od", "seafoodrtt")
	fmt.Println(matched, err)
	t.Error("end")
}
