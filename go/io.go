/*
Utilities and containers for IO.

References:
- https://github.com/gocarina/gocsv
- https://go.dev/blog/json
*/
package main

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"gonum.org/v1/gonum/mat"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// readDenseVectorFile reads field fastText vectors from a TSV
func readDenseVectorFile(path string, meta *Meta) *mat.Dense {
	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("Could not %v", err)
	}
	defer file.Close()
	csvReader := csv.NewReader(file)
	csvReader.Comma = '\t'
	vectors := make([][]float64, 0)
	vectorIndex := make([]int, 0)
	for {
		line, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		// The first column gives the field ID
		fieldId, _ := strconv.Atoi(line[0])
		// The remaining columns give vector elements
		vector := make([]float64, len(line)-1)
		for j, s := range line[1:len(line)] {
			element, _ := strconv.ParseFloat(s, 64)
			vector[j] = element
		}
		vectors = append(vectors, vector)
		vectorIndex = append(vectorIndex, fieldId)
	}
	// Put the array in order
	sort.Slice(vectors, func(i, j int) bool {
		return vectorIndex[i] < vectorIndex[j]
	})
	// Do the same for the vectorIndex
	sort.Slice(vectorIndex, func(i, j int) bool {
		return vectorIndex[i] < vectorIndex[j]
	})
	// Validate
	for i, field := range meta.Fields {
		if vectorIndex[i] != field.Id {
			log.Fatal(field.Id, vectorIndex[i])
		}
	}
	if len(vectors) != meta.FieldCount {
		log.Fatalf("Loaded %d dense field vectors from %s; expected %d", len(vectors), path, meta.FieldCount)
	}
	// We just created an array of arrays, but for a new matrix need a single array of floats
	var data []float64
	for _, x := range vectors {
		for _, y := range x {
			data = append(data, y)
		}
	}
	m := mat.NewDense(len(vectors), len(vectors[0]), data)
	return m
}

// readSparseVectorFile reads field tf-idf vectors from JSON
func readSparseVectorFile(path string, meta *Meta) []FieldVector {
	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("Could not %v", err)
	}
	defer file.Close()
	// We'll read line by line
	scanner := bufio.NewScanner(file)
	const maxLineSize = 1_000_000
	buf := make([]byte, maxLineSize)
	scanner.Buffer(buf, maxLineSize)
	var vectors []FieldVector
	for scanner.Scan() {
		// Parse a line of JSON
		var field = FieldVector{}
		s := scanner.Text()
		err := jsoniter.Unmarshal([]byte(s), &field)
		if err != nil {
			log.Fatal(err)
		}
		field.Sort()
		vectors = append(vectors, field)
	}
	if len(vectors) != meta.FieldCount {
		log.Fatalf("Loaded %d sparse field vectors from %s; expected %d", len(vectors), path, meta.FieldCount)
	}
	return vectors
}

// Doc holds input documents
type Doc struct {
	MergedId string `json:"merged_id"`
	Text     string `json:"text"`
	Err      error
}

// TSVHeader creates a header for a field score TSV file
func (meta *Meta) TSVHeader(scoreColumn bool) string {
	// The first column gives document IDs
	header := []string{"merged_id"}
	if scoreColumn {
		// We have a second "score" column for indicating what kind of scores the row contains
		header = append(header, "score")
	}
	// The remaining columns give field IDs
	for _, field := range meta.Fields {
		header = append(header, strconv.Itoa(field.Id))
	}
	return strings.Join(header, outputDelimiter)
}

// WriteTSVHeader writes a field score header to a file
func (meta *Meta) WriteTSVHeader(file *os.File, scoreColumn bool) {
	_, err := file.WriteString(meta.TSVHeader(scoreColumn) + "\n")
	if err != nil {
		log.Fatal(err)
	}
}

// MarshalTSV marshals docScores into TSV
func (docScores *DocScores) MarshalTSV(label string) string {
	row := NewTSVRow(docScores.MergedId, label)
	for _, score := range docScores.Scores {
		row = append(row, strconv.FormatFloat(score, 'f', outputPrecision, 64))
	}
	return strings.Join(row, outputDelimiter)
}

// MarshalVectorTSV marshals docScore dense vectors (fastText, entity fastText) into TSV
func (docScores *DocScores) MarshalVectorTSV(label string, vector *mat.VecDense) string {
	row := NewTSVRow(docScores.MergedId, label)
	// Surely a better way?
	for i := 0; i < vector.Len(); i++ {
		row = append(row, strconv.FormatFloat(vector.AtVec(i), 'f', outputPrecision, 64))
	}
	return strings.Join(row, outputDelimiter)
}

func NewTSVRow(docId string, label string) []string {
	var row = []string{docId}
	if label != "" {
		row = append(row, label)
	}
	return row
}

// ReadInputs iterates over lines in one or more input files (possibly gzipped)
func ReadInputs(path string) <-chan []byte {
	lines := make(chan []byte)
	matches, err := filepath.Glob(path)
	if err != nil {
		log.Fatal(err)
	}
	if len(matches) == 0 {
		log.Fatalf("No inputs matched %s", path)
	}
	go func() {
		for _, match := range matches {
			// Open the input file
			file, err := os.Open(match)
			if err != nil {
				log.Fatalf("Could not %v", err)
			}

			var scanner *bufio.Scanner

			// We just check for a .gz extension rather than inspecting the file header
			if strings.HasSuffix(match, ".gz") {
				reader, err := gzip.NewReader(file)
				if err != nil {
					log.Fatal(err)
				}
				scanner = bufio.NewScanner(reader)
			} else {
				scanner = bufio.NewScanner(file)
			}

			const maxLineSize = 10_000_000 // Don't choke on large lines
			buf := make([]byte, maxLineSize)
			scanner.Buffer(buf, maxLineSize)

			log.Printf("Reading %s", match)
			for scanner.Scan() {
				// Read a line of input
				bytes := scanner.Bytes()
				if len(bytes) == 0 {
					continue
				}
				lines <- bytes
			}

			err = file.Close()
			if err != nil {
				log.Fatal(err)
			}
		}
		// Generator pattern: close the channel after iterating over all inputs
		close(lines)
	}()
	return lines
}

type DocScoresOutput struct {
	Id     string             `json:"merged_id"`
	Fields []FieldScoreOutput `json:"fields"`
}

type FieldScoreOutput struct {
	Id    int     `json:"id"`
	Score float64 `json:"score"`
}

func ReshapeOutput(inputPath string, outputPath string) {
	fileIn, err := os.Open(inputPath)
	if err != nil {
		log.Fatalf("Could not %v", err)
	}
	defer func(fileIn *os.File) {
		_ = fileIn.Close()
	}(fileIn)

	records := make(chan []string, maxWorker)
	inputDone := make(chan bool)
	headerOutput := make(chan []string)
	var wg sync.WaitGroup
	go func() {
		err = TSVToChanMaps(fileIn, records, headerOutput, &wg)
		if err != nil {
			log.Fatal(err)
		}
		close(records)
		inputDone <- true
	}()

	header := <-headerOutput
	for w := 0; w < maxWorker; w++ {
		go func(workerId int) {
			var fileOut *os.File
			if outputPath == "" {
				// Write output to stdout if no path specified
				fileOut = os.Stdout
			} else {
				// Otherwise open the output file
				if maxWorker > 1 {
					index := fmt.Sprintf("_%03d", workerId)
					fileOut, err = os.Create(outputPath + index + ".jsonl")
				} else {
					fileOut, err = os.Create(outputPath)
				}
				if err != nil {
					log.Fatalf("Could not %v", err)
				}
				defer func(fileOut *os.File) {
					_ = fileOut.Close()
				}(fileOut)
			}

			for record := range records {
				output := DocScoresOutput{
					Id:     record[0],
					Fields: nil,
				}
				for i := 1; i < len(header); i++ {
					fieldId, err := strconv.Atoi(header[i])
					if err != nil {
						log.Fatal(err)
					}
					fieldScore, err := strconv.ParseFloat(record[i], 64)
					output.Fields = append(output.Fields, FieldScoreOutput{
						Id:    fieldId,
						Score: fieldScore,
					})
				}
				bytes, err := json.Marshal(output)
				if err != nil {
					log.Fatal(err)
				}
				_, err = fileOut.Write(bytes)
				if err != nil {
					log.Fatal(err)
				}
				_, err = fileOut.WriteString("\n")
				if err != nil {
					log.Fatal(err)
				}
				wg.Done()
			}
		}(w)
	}
	wg.Wait()
	<-inputDone
}

// TSVToChanMaps parses the TSV from the reader and send a dictionary in the chan c, using the header row as the keys.
func TSVToChanMaps(reader io.Reader, c chan<- []string, headerOutput chan<- []string, waitGroup *sync.WaitGroup) error {
	r := csv.NewReader(reader)
	r.Comma = '\t'
	var header []string
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if header == nil {
			header = record
			headerOutput <- record
		} else {
			c <- record
			waitGroup.Add(1)
		}
	}
	return nil
}
