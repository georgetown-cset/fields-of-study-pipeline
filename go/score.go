/*
Calculate field scores for documents.
*/
package main

import (
	"bufio"
	"encoding/json"
	"github.com/cheggaaa/pb/v3"
	"log"
	"os"
	"time"
)

func Score() {
	var f *os.File
	var err error
	if outputPath == "" {
		// Write to stdout if no output path specified
		f = os.Stdout
	} else {
		// Open the output file
		f, err = os.Create(outputPath)
		if err != nil {
			log.Fatalf("Could not %v", err)
		}
		defer func(f *os.File) {
			_ = f.Close()
		}(f)
	}
	meta := NewMeta()
	meta.WriteTSVHeader(f, outputAll)

	file, err := os.Open(inputPath)
	if err != nil {
		log.Fatalf("Could not %v", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	const maxLineSize = 1_000_000 // Don't choke on large lines
	buf := make([]byte, maxLineSize)
	scanner.Buffer(buf, maxLineSize)

	// Set up the worker pool
	dispatcher := NewDispatcher(maxWorker, outputAll)
	dispatcher.Run()
	JobQueue = make(chan Job, maxQueue)
	ResultQueue = make(chan DocScores, maxQueue)

	start := time.Now()
	log.Printf("Reading inputs")

	var doc Doc
	jobCount := 0
	skipCount := 0
	for scanner.Scan() {
		// Read a line of input
		s := scanner.Text()
		if s == "" {
			continue
		}

		// Unmarshal the JSON into a Doc. If this takes any non-trivial time, it could be done by the worker instead
		err := json.Unmarshal([]byte(s), &doc)
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
			_, err := f.WriteString(scores.MarshalTSV("") + "\n")
			if err != nil {
				log.Fatal(err)
			}
		} else {
			_, err := f.WriteString(scores.MarshalVectorTSV("fastText", scores.FastTextScores) + "\n")
			if err != nil {
				log.Fatal(err)
			}
			_, err = f.WriteString(scores.MarshalVectorTSV("entity", scores.EntityScores) + "\n")
			if err != nil {
				log.Fatal(err)
			}
			_, err = f.WriteString(scores.MarshalVectorTSV("tfidf", scores.TfidfScores) + "\n")
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
