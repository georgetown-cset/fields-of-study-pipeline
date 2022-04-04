/*
Calculate field scores for documents.
*/
package main

import (
	json "github.com/json-iterator/go"
	"log"
	"os"
	"time"
)

func Score() {
	// Open the output
	var fout *os.File
	var err error
	if outputPath == "" {
		// Write output to stdout if no path specified
		fout = os.Stdout
	} else {
		// Otherwise open the output file
		fout, err = os.Create(outputPath)
		if err != nil {
			log.Fatalf("Could not %v", err)
		}
		defer func(fout *os.File) {
			_ = fout.Close()
		}(fout)
	}

	// Set up the worker pool
	dispatcher := NewDispatcher(maxWorker, outputAll)
	dispatcher.Run()
	JobQueue = make(chan Job, maxWorker)
	ResultQueue = make(chan *DocScores, maxWorker)

	start := time.Now()
	log.Printf("Reading inputs")

	var doc Doc
	jobCount := 0
	skipCount := 0

	inputDone := make(chan bool)
	inputQueue := ReadInputs(inputPath)
	go func() {
		for input := range inputQueue {
			if len(input) == 0 {
				continue
			}
			// Unmarshal the JSON into a Doc. If this takes any non-trivial time, it could be done by the worker instead
			err := json.Unmarshal(input, &doc)
			if err != nil {
				log.Fatal(err)
			}

			// Input JSON with empty text isn't an Unmarshal error, so we have to check for it. Otherwise, there will be
			// fewer outputs than expected from the ResultQueue, leading to an out-of-bounds panic. Ideally we'd be sending
			// a Doc instance back to the ResultQueue with its Error field populated, but this isn't happening somewhere.
			if doc.Text == "" {
				skipCount += 1
				continue
			}

			// Send the doc to the worker pool via the job queue
			JobQueue <- Job{Doc: doc}
			jobCount += 1
		}
		inputDone <- true
	}()

	outputDone := make(chan bool)
	outputCount := 0
	inputIsDone := false
	go func() {
		if err != nil {
			log.Fatal("Can't create file", err)
		}
		// Read scores from the result channel as they become available
		for {
			scores := <-ResultQueue
			obj, err := json.MarshalToString(&scores)
			_, err = fout.WriteString(obj + "\n")
			if err != nil {
				return
			}
			if err != nil {
				log.Fatal(err)
			}

			outputCount++

			if showProgress {
				if outputCount%10000 == 0 {
					log.Printf("%d", outputCount)
				}
			}
			if inputIsDone && jobCount == outputCount {
				outputDone <- true
			}
			if err != nil {
				log.Println("WriteStop error", err)
			}
		}
	}()
	// Wait for remaining inputs
	inputIsDone = <-inputDone
	log.Printf("%d jobs", jobCount)

	// Wait for remaining outputs
	<-outputDone

	if jobCount == 0 {
		log.Fatalf("No input texts for '-i %s'", inputPath)
	}
	if skipCount > 0 {
		log.Printf("Skipped %d inputs without text", skipCount)
	}

	elapsed := time.Since(start)
	log.Printf("Finished scoring after %s", elapsed)
}
