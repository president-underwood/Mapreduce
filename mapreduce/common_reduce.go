package mapreduce

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
)


func doReduce(
	jobName string, // 整个作业的名字
	reduceTaskNumber int, // 名字
	nMap int, //map发生的数量
	reduceF func(key string, values []string) string,
) {
	inputFiles := make([] *os.File, nMap)
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTask)
		inputFiles[i], _ = os.Open(fileName)
	}

	// 收集key/value 键值对
	KeyValues := make(map[string][]string)
	for _, inputFile := range inputFiles {
		defer inputFile.Close()
		dec := json.NewDecoder(inputFile)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			KeyValues[kv.Key] = append(KeyValues[kv.Key], kv.Value)
		}
	}
	keys := make([]string, 0, len(KeyValues))
	for k := range KeyValues {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	
	//输出文件
	out, err := os.Create(outFile)
	if err != nil {
		log.Fatal("Error in creating file", outFile)
	}
	defer out.Close()

	enc := json.NewEncoder(out)
	for _, key := range keys {
		kv := KeyValue{key, reduceF(key, KeyValues[key])}
		enc.Encode(kv)
	}
}

