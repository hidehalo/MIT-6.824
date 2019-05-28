package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	fmt.Printf("Schedule: %v done\n", phase)

	var wg sync.WaitGroup

	var args DoTaskArgs

	idle := make(chan string)

	exit := make(chan bool)

	tasks := make(chan DoTaskArgs)

	wg.Add(ntasks)

	// Receive existing registered workers (if any) and new ones as they register to idle channel
	go func() {
		for worker := range registerChan {
			idle <- worker
		}
	}()

	// Exit procedure
	go func() {
		wg.Wait()

		exit <- true
	}()

	// Assign a task to any idle worker
	for taskNumber := 0; taskNumber < ntasks; taskNumber++ {
		if phase == mapPhase {
			args = DoTaskArgs{
				JobName:       jobName,
				File:          mapFiles[taskNumber],
				Phase:         phase,
				TaskNumber:    taskNumber,
				NumOtherPhase: n_other,
			}
		} else {
			args = DoTaskArgs{
				JobName:       jobName,
				Phase:         phase,
				TaskNumber:    taskNumber,
				NumOtherPhase: n_other,
			}
		}
		go func (args DoTaskArgs) {
			tasks <- args
		}(args)
	}

	for {
		select {
		case args := <- tasks:
		// Worker.DoTask procedure
		go func(args DoTaskArgs) {
			worker := <-idle
			ret := call(worker, "Worker.DoTask", args, nil)
			if ret {
				wg.Done()
			} else {
				tasks <- args
			}
			// Put worker back to idle channel
			idle <- worker
		}(args)
		case <-exit:
			return
		}
	}
}
