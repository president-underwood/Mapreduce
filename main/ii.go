package main

import (
	"os"
	"sort"
	"strconv"
	"strings"
	"unicode"
)
import "fmt"
import "mapreduce"

func mapF(document string, value string) (res []mapreduce.KeyValue) {
	
	f := func(c rune) bool {
		return !unicode.IsLetter(c)
	}
	var strSlice []string = strings.FieldsFunc(value,f)
	var kvSlice []mapreduce.KeyValue
	for _,str := range strSlice {
		kvSlice = append(kvSlice, mapreduce.KeyValue{str, document})
	}

	return kvSlice

}


func reduceF(key string, values []string) string {
	
	var cnt int64
	var documents string
	set := make(map[string]bool)
	for _,str := range values{
		set[str] = true
	}
	var keys []string
	for key := range set{
		if set[key] == false{
			continue
		}
		keys = append(keys,key)
	}
	sort.Strings(keys)
	for _,key := range keys{
		cnt++
		if cnt >= 2{
			documents += ","
		}
		documents += key
	}
	return strconv.FormatInt(cnt,10) + " " + documents
}

//三种模式
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("iiseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("iiseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100)
	}
}
