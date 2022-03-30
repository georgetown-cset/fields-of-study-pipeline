/*
Utilities and containers for IO.

References:
- https://github.com/gocarina/gocsv
- https://go.dev/blog/json
*/
package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"gonum.org/v1/gonum/mat"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
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
		err := json.Unmarshal([]byte(s), &field)
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
