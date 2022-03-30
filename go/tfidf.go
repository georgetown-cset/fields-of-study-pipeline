/*
Create and compare tf-idf document vectors.
*/
package main

import (
	"encoding/csv"
	"github.com/james-bowman/nlp"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
)

// Tfidf holds a term-value pair
type Tfidf struct {
	Id    int     `json:"id"`
	Value float64 `json:"value"`
}

// FieldVector holds the tf-idf vector for a field
type FieldVector struct {
	Id     int     `json:"id"`
	Vector []Tfidf `json:"vector"`
}

// Sort sorts a FieldVector by term ID as required for scoring
func (fv *FieldVector) Sort() {
	sort.Slice(fv.Vector, func(i, j int) bool {
		return fv.Vector[i].Id < fv.Vector[j].Id
	})
}

// TfidfFieldScore holds a field-score tuple for sparse vector scoring
type TfidfFieldScore struct {
	Field int
	Score float64
}

// TfidfVectorizer creates tf-idf embeddings for text
type TfidfVectorizer struct {
	countVectorizer *nlp.CountVectoriser
	weights         *[]float64
}

// NewTfidfVectorizer constructs a TfidfVectorizer by reading a gensim dictionary file
func NewTfidfVectorizer() *TfidfVectorizer {
	// The text file is a TSV defining our vocab
	f, err := os.Open(paths.Vocab())
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	csvReader := csv.NewReader(f)
	csvReader.Comma = '\t'
	// The first row has a single non-header value, the size of the corpus
	csvReader.FieldsPerRecord = -1
	header, err := csvReader.Read()
	if err != nil {
		log.Fatal(err)
	}
	corpusSize, _ := strconv.ParseInt(header[0], 10, 0)

	// Read the rest, each line a triple: term ID, text, and frequency
	data, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	// The CountVectoriser owns a vocab that has to include each of our terms, or they won't be counted
	vectorizer := nlp.NewCountVectoriser()
	// The vectoriser's module has a tfidf transformer class, but we're handling weighting ourselves because
	// it's much faster not to deal with its sparse matrix output
	frequencies := make(map[int]int)
	maxId := 0
	for _, line := range data {
		// Parse a row
		termId, _ := strconv.Atoi(line[0])
		termText := line[1]
		termFrequency, _ := strconv.Atoi(line[2])

		// Vocabulary is a map of terms to term IDs
		vectorizer.Vocabulary[termText] = int(termId)
		frequencies[int(termId)] = int(termFrequency)

		// Keep track of the largest observed term ID, so below we can allocate a large enough weights vector
		if termId > maxId {
			maxId = termId
		}
	}

	// We calculate IDFs here using the corpus size read from the first line of the gensim dictionary file and the term
	// frequencies read from each subsequent row
	weights := make([]float64, maxId+1)
	for _, termId := range vectorizer.Vocabulary {
		if termFreq, ok := frequencies[termId]; ok {
			weights[termId] = math.Log(float64(1+corpusSize) / float64(1+termFreq))
		} else {
			panic("unexpected termID")
		}
	}
	return &TfidfVectorizer{vectorizer, &weights}
}

// Vectorize creates a tf-idf vector from document text
func (v *TfidfVectorizer) Vectorize(doc string) *[]Tfidf {
	// Iterate over the text yielding weighted term counts
	bow := make(map[int]float64)
	v.countVectorizer.Tokeniser.ForEachIn(doc, func(word string) {
		i, exists := v.countVectorizer.Vocabulary[word]
		if exists {
			// Term is in vocab
			bow[i] += (*v.weights)[i]
		}
	})
	// Calculate the l2 norm
	var sumOfSquares float64
	for _, x := range bow {
		sumOfSquares += math.Pow(x, 2)
	}
	norm := math.Sqrt(sumOfSquares)
	if norm == 0 {
		// Avoid divide-by-zero
		norm = 1
	}
	// Last step is to divide each tf-idf by the l2 norm, which requires another iteration over the bow.
	// In doing this we take the opportunity to restructure the data as an array of 'Tfidf' structs, sorted by token
	// ID, because this will be necessary when calculating cosine similarities between doc vectors and fields.
	tfidfs := make([]Tfidf, len(bow))
	// Iterating over the bow in order of token ID requires a key slice
	keys := make([]int, 0, len(bow))
	for k, _ := range bow {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	// Now we iterate over the key slice to fill the Tfidf array
	for i, k := range keys {
		tfidfs[i] = Tfidf{k, bow[k] / norm}
	}
	return &tfidfs
}

// scoreSparse calculates the cosine similarity of tf-idf vectors
func scoreSparse(doc *[]Tfidf, fields *[][]Tfidf, fieldIndex []int) []TfidfFieldScore {
	var cosines []TfidfFieldScore
	n := len(*doc)
	if n == 0 {
		// No in-vocab tokens in doc
		return cosines
	}
	var i int
	var j int
	var cosine float64
	// This depends on the tf-idf vectors being sorted by term ID
	for k, field := range *fields {
		cosine = 0.0
		m := len(field)
		i = 0
		j = 0
		for {
			if (*doc)[i].Id == field[j].Id {
				// We have a common term
				cosine += (*doc)[i].Value * field[j].Value
				i++
				j++
			} else if (*doc)[i].Id > field[j].Id {
				j++
			} else {
				i++
			}
			if i == n || j == m {
				break
			}
		}
		if cosine > 0 {
			cosines = append(cosines, TfidfFieldScore{fieldIndex[k], cosine})
		}
	}
	return cosines
}
