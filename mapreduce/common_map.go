package mapreduce

import (
	"hash/fnv"
	"io/ioutil"
	"log"
	"encoding/json"
	"os"
)


func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// read contents from 'infile'
	dat,err := ioutil.ReadFile(inFile)
	if err != nil {
		log.Fatal("doMap: readFile ", err)
	}

	//transfer data into ‘kvSlice’ according to the mapF()
	kvSlice := mapF(inFile,string(dat))

	//divide the ‘kvSlice’ into 'reduceKv' according to the ihash()
	var reduceKv [][]KeyValue // temporary variable which will be written into reduce files
	for i:=0;i<nReduce;i++ {
		s1 := make([]KeyValue,0)
		reduceKv = append(reduceKv, s1)
	}
	for _,kv := range kvSlice{
		hash := int(ihash(kv.Key)) % nReduce
		reduceKv[hash] = append(reduceKv[hash],kv)
	}

	//write 'reduceKv' into ‘nReduce’ JSON files
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
	return h.Sum32()
}
