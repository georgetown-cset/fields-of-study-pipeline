/*
Create and compare entity vectors.

References:
- https://github.com/petar-dambovaliev/aho-corasick/
*/
package main

import (
	"encoding/csv"
	"github.com/petar-dambovaliev/aho-corasick"
	"gonum.org/v1/gonum/mat"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

// EntityVectorizer creates entity-mention-based fastText embeddings for text
type EntityVectorizer struct {
	Matcher *aho_corasick.AhoCorasick
	Matrix  *mat.Dense
}

// NewEntityVectorizer loads an EntityVectorizer from the disk
func NewEntityVectorizer() *EntityVectorizer {
	meta := NewMeta()
	matcher := loadMatcher(meta.Fields)
	entityMatrix := readDenseVectorFile(paths.FieldEntityVectors(), meta)
	return &EntityVectorizer{&matcher, entityMatrix}
}

// Vectorize creates an entity-mention fastText vector from document text
func (e *EntityVectorizer) Vectorize(doc *Doc) *mat.VecDense {
	v := mat.NewVecDense(entityDim, nil)
	v.Zero()
	matches := e.Matcher.FindAll(doc.Text)
	for _, match := range matches {
		v.AddVec(v, e.Matrix.RowView(match.Pattern()))
	}
	v = NormVector(v)
	return v
}

// loadMatcher creates an Aho-Corasick automaton for identifying field mentions in text
func loadMatcher(fieldMetas []*FieldMeta) aho_corasick.AhoCorasick {
	// Load the entity search terms for each field from TSV
	file, err := os.Open(paths.EntityTrie())
	if err != nil {
		log.Fatalf("Could not %v", err)
	}
	defer file.Close()
	csvReader := csv.NewReader(file)
	csvReader.Comma = '\t'
	idToTerm := make(map[int]string)
	for {
		line, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		fieldId, _ := strconv.Atoi(line[0])
		idToTerm[fieldId] = strings.ToLower(line[1])
	}
	// The matcher will return the insertion-order index of each match. We'll then need to look up the corresponding
	// entity vector, by row index. So we need the insertion-order index here to match the entity matrix row index
	var orderedTerms []string
	for _, field := range fieldMetas {
		orderedTerms = append(orderedTerms, idToTerm[field.Id])
	}

	builder := aho_corasick.NewAhoCorasickBuilder(aho_corasick.Opts{
		AsciiCaseInsensitive: true,
		MatchOnlyWholeWords:  true,
		MatchKind:            aho_corasick.LeftMostLongestMatch,
		DFA:                  true,
	})

	ac := builder.Build(orderedTerms)
	return ac
}
