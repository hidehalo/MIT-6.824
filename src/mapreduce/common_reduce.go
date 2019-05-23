package mapreduce

import (
	"encoding/json"
	"os"
	"sort"
	"strings"
)

// ByKey Sort KeyValue array by KeyValue.Key
type ByKey []KeyValue

func (kv ByKey) Len() int           { return len(kv) }
func (kv ByKey) Swap(i, j int)      { kv[i], kv[j] = kv[j], kv[i] }
func (kv ByKey) Less(i, j int) bool { return strings.Compare(kv[i].Key, kv[j].Key) == -1 }

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
	data := make([]KeyValue, 0, 1000)
	pairs := make(map[string][]string)

	for mapTask := 0; mapTask < nMap; mapTask++ {
		inFile := reduceName(jobName, mapTask, reduceTask)
		input, err := os.Open(inFile)
		checkError(err)
		dec := json.NewDecoder(input)
		for dec.More() {
			var kv KeyValue
			err := dec.Decode(&kv)
			checkError(err)
			data = append(data, kv)
		}
		input.Close()
	}

	sort.Sort(ByKey(data))

	for _, kv := range data {
		if _, ex := pairs[kv.Key]; ex != true {
			pairs[kv.Key] = make([]string, 0, 1000)
		}
		pairs[kv.Key] = append(pairs[kv.Key], kv.Value)
	}

	ret := make([]KeyValue, 0, 1000)
	for key, values := range pairs {
		ret = append(ret, KeyValue{
			key,
			reduceF(key, values),
		})
	}
	output, err := os.OpenFile(outFile, os.O_CREATE|os.O_WRONLY, 0666)
	checkError(err)
	defer output.Close()
	enc := json.NewEncoder(output)
	for _, kv := range ret {
		err := enc.Encode(kv)
		checkError(err)
	}
}
