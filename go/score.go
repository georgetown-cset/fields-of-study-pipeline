/*
Calculate field scores for documents.
*/
package main

import (
	"encoding/json"
	"github.com/cheggaaa/pb/v3"
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
	// Write headers to output
	meta := NewMeta()
	meta.WriteTSVHeader(fout, outputAll)

	inputQueue := ReadInputs(inputPath)

	// Set up the worker pool
	dispatcher := NewDispatcher(maxWorker, outputAll)
	dispatcher.Run()
	JobQueue = make(chan Job, maxWorker)
	ResultQueue = make(chan DocScores, maxWorker)

	start := time.Now()
	log.Printf("Reading inputs")

	var doc Doc
	jobCount := 0
	skipCount := 0

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

	// No work is being done yet so this loop finishes immediately
	if jobCount == 0 {
		log.Fatalf("No input texts for '-i %s'", inputPath)
	} else {
		log.Printf("Added %d inputs to buffer", jobCount)
	}
	if skipCount > 0 {
		log.Printf("Skipped %d inputs without text", skipCount)
	}

	var bar *pb.ProgressBar
	if showProgress {
		// Start a progress bar
		bar = pb.StartNew(jobCount)
		bar.SetRefreshRate(time.Second)
		err = bar.Err()
		if err != nil {
			log.Fatal(err)
		}
	}

	// Read scores from the result channel as they become available
	for i := 1; i <= jobCount; i++ {
		scores := <-ResultQueue
		if !outputAll {
			_, err := fout.WriteString(scores.MarshalTSV("") + "\n")
			if err != nil {
				log.Fatal(err)
			}
		} else {
			_, err := fout.WriteString(scores.MarshalVectorTSV("fastText", scores.FastTextScores) + "\n")
			if err != nil {
				log.Fatal(err)
			}
			_, err = fout.WriteString(scores.MarshalVectorTSV("entity", scores.EntityScores) + "\n")
			if err != nil {
				log.Fatal(err)
			}
			_, err = fout.WriteString(scores.MarshalVectorTSV("tfidf", scores.TfidfScores) + "\n")
			if err != nil {
				log.Fatal(err)
			}
		}
		if showProgress {
			bar.Increment()
		}
		// Exit after receiving as many results as we had jobs
	}

	if showProgress {
		bar.Finish()
	}

	elapsed := time.Since(start)
	log.Printf("Finished scoring after %s", elapsed)
}
