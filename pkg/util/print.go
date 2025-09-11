package util

import (
	"fmt"
	"reflect"
)

func PrintStruct[T any](t T) {
	nodeVal := reflect.ValueOf(t)
	nodeType := nodeVal.Type()
	if nodeVal.Kind() == reflect.Ptr {
		nodeVal = nodeVal.Elem()
		nodeType = nodeVal.Type()
	}
	for i := 0; i < nodeVal.NumField(); i++ {
		field := nodeType.Field(i)
		value := nodeVal.Field(i).Interface()
		fmt.Printf("%s: %v\n", field.Name, value)
	}
}
