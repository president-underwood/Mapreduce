package mapreduce

import (
	"hash/fnv"
	"io/ioutil"
	"log"
	"encoding/json"
	"os"
)


func doMap(
	jobName string, // 整个mapreduce任务的名字
	mapTaskNumber int, // map 任务的数量
	inFile string,
	nReduce int, // 需要执行的reduce数量
	mapF func(file string, contents string) []KeyValue,
) {
	//读取文件流
	dat,err := ioutil.ReadFile(inFile)
	if err != nil {
		log.Fatal("doMap: readFile ", err)
	}

	kvSlice := mapF(inFile,string(dat))

	var reduceKv [][]KeyValue 
	for i:=0;i<nReduce;i++ {
		s1 := make([]KeyValue,0)
		reduceKv = append(reduceKv, s1)
	}
	for _,kv := range kvSlice{
		hash := int(ihash(kv.Key)) % nReduce
		reduceKv[hash] = append(reduceKv[hash],kv)
	}

	for i := 0;i<nReduce;i++ {
		file,err := os.Create(reduceName(jobName,mapTaskNumber,i))
		if err != nil {
			log.Fatal("doMap: create ", err)
		}

		enc := json.NewEncoder(file)
		for _, kv := range reduceKv[i]{
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal("doMap: json encodem ", err)
			}
		}

		file.Close()

	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return uint(h.Sum32()& 0x7fffffff)
}
