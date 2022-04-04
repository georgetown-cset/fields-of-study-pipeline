/*
Define fields and their embeddings, for comparison against document embeddings.
*/
package main

import (
	"bufio"
	"encoding/csv"
	"github.com/gocarina/gocsv"
	"gonum.org/v1/gonum/mat"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
)

// Scorer compares field embeddings and document embeddings, yielding field scores
type Scorer struct {
	Meta            *Meta
	FastTextVectors *mat.Dense
	TfidfArray      *[][]Tfidf
	TfidfIndex      []int
	EntityVectors   *mat.Dense
	IncludeAll      bool
}

// NewScorer construct a Scorer
func NewScorer(includeAll bool) *Scorer {
	meta := NewMeta()
	tfidfVectors := readSparseVectorFile(paths.FieldTfidfVectors(), meta)
	tfidfArray := make([][]Tfidf, len(tfidfVectors))
	tfidfIndex := make([]int, len(tfidfVectors))
	tfidfReverseIndex := make(map[int]int, len(tfidfVectors))
	for i, x := range tfidfVectors {
		tfidfIndex[i] = x.Id
		tfidfReverseIndex[x.Id] = i
		tfidfArray[i] = x.Vector

	}
	entityVectors := readDenseVectorFile(paths.FieldEntityVectors(), meta)
	fastTextVectors := readDenseVectorFile(paths.FieldFastTextVectors(), meta)

	scorer := Scorer{
		meta,
		fastTextVectors,
		&tfidfArray,
		tfidfIndex,
		entityVectors,
		includeAll,
	}
	log.Print("Loaded assets")
	return &scorer
}

// Score compares field and document embeddings, yielding field scores
func (scorer *Scorer) Score(docEmbedding *DocEmbedding) *DocScores {
	fastTextScores := mat.NewVecDense(scorer.Meta.FieldCount, nil)
	fastTextScores.MulVec(scorer.FastTextVectors, docEmbedding.FastText)

	tfidf := scoreSparse(docEmbedding.Tfidf, scorer.TfidfArray, scorer.TfidfIndex)
	tfidfScores := mat.NewVecDense(scorer.Meta.FieldCount, nil)
	for i, x := range tfidf {
		tfidfScores.SetVec(i, x.Score)
	}

	entityScores := mat.NewVecDense(scorer.Meta.FieldCount, nil)
	entityScores.MulVec(scorer.EntityVectors, docEmbedding.Entity)

	scores := DocScores{
		MergedId: docEmbedding.MergedId,
		Scores:   scorer.average(fastTextScores, tfidfScores, entityScores),
	}
	if scorer.IncludeAll {
		scores.FastTextScores = fastTextScores
		scores.TfidfScores = tfidfScores
		scores.EntityScores = entityScores
	}
	return &scores
}

// average calculates field scores by averaging over the scores from the three embedding methods
func (scorer *Scorer) average(scores ...*mat.VecDense) []FieldScoreOutput {
	avg := make([]FieldScoreOutput, scorer.Meta.FieldCount)
	var score, n, sum float64
	/*
		We average the available scores for each field, excluding missing values. This isn't quite faithful to the
		original implementation, which used a weighted average.

		In LanguageSimilarity.cs, if word2vec and entity similarities are both available, we have:

			weightedCosine = (1.0 - entityWeight) * wordEmbCosine + entityWeight * entityEmbCosine;

		Then if tf-idf embeddings are available:

			float bowCosine = Vector.SparseCosine(textBowVec, fosInfo.TfIdf);
		   	weightedCosine = (1.0 - this.bowWeight) * weightedCosine + this.bowWeight * bowCosine;

		bowWeight and entityWeight default to 0.5. So when all similarities are available, the tf-idf vector receives
		twice the weight as the word2vec and entity embeddings.
	*/
	for i := 0; i < scorer.Meta.FieldCount; i++ {
		sum, n = 0.0, 0.0
		// Get the ith score from each vector
		for _, vector := range scores {
			// The ith element in the score vector is the cosine similarity between the doc and the ith field
			score = vector.AtVec(i)
			// The zero value for a float64 is 0, so a score is non-missing if it's positive
			if score > 0 {
				sum += score
				n += 1
			}
		}
		if n > 0 {
			// average the n available scores
			avg[i] = FieldScoreOutput{scorer.Meta.Keys[i], sum / n}
		} else {
			// If no scores are available, the field score is zero
			avg[i] = FieldScoreOutput{scorer.Meta.Keys[i], sum}
		}
	}
	return avg
}

// FieldMeta describes a field
type FieldMeta struct {
	Id             int    `csv:"id"`
	Level          int    `csv:"level"`
	DisplayName    string `csv:"display_name"`
	NormalizedName string `csv:"normalized_name"`
	WikiTitle      string `csv:"wiki_title"`
}

// Meta holds fields' metadata
type Meta struct {
	Fields     []*FieldMeta
	Keys       []int
	FieldCount int
}

// NewMeta constructs a new Meta instance
func NewMeta() *Meta {
	fields := loadMeta()
	keys := loadKeys()
	if len(fields) > len(keys) {
		// We have metadata for more fields than are in the field index, so try dropping from metadata fields that we
		// don't see in the index. This is expected when for ZH we may have fewer fields defined than in EN
		subset := make([]*FieldMeta, 0)
		for i := 0; i < len(fields); i++ {
			if contains(keys, fields[i].Id) {
				subset = append(subset, fields[i])
			}
		}
		log.Printf("Using metadata for %d fields out of %d available", len(subset), len(fields))
		fields = subset
	}
	if len(fields) != len(keys) {
		log.Fatalf("Field index and metadata should be the same size, but got %d != %d", len(fields), len(keys))
	}
	// We should now have fields and metadata in the same order
	for i, field := range fields {
		if field.Id != keys[i] {
			log.Fatalf("Field index and metadata should be ordered consistently, but got %d != %d at index %d",
				field.Id, keys[i], i)
		}
	}
	return &Meta{fields, keys, len(keys)}
}

func contains(s []int, i int) bool {
	// https://stackoverflow.com/a/10485970
	for _, a := range s {
		if a == i {
			return true
		}
	}
	return false
}

// loadKeys loads a field key order file from the disk
func loadKeys() []int {
	var fieldIds []int
	file, err := os.Open(paths.FieldKey())
	if err != nil {
		log.Fatalf("Could not %v", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		s := scanner.Text()
		id, err := strconv.Atoi(s)
		if err != nil {
			log.Fatal(err)
		}
		fieldIds = append(fieldIds, id)
	}
	return fieldIds
}

// loadMeta loads field metadata from the disk
func loadMeta() []*FieldMeta {
	file, err := os.Open(paths.FieldMeta())
	if err != nil {
		log.Fatalf("Could not %v", err)
	}
	defer file.Close()

	var fields []*FieldMeta
	gocsv.SetCSVReader(func(in io.Reader) gocsv.CSVReader {
		r := csv.NewReader(in)
		r.Comma = '\t'
		return r
	})
	if err := gocsv.UnmarshalFile(file, &fields); err != nil {
		log.Fatalf("could not unmarshal: %v", err)
	}

	// Sort the fields in order of ID
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].Id < fields[j].Id
	})
	return fields
}

// DocScores holds the field scores associated with a document
type DocScores struct {
	MergedId       string             `json:"merged_id"`
	Scores         []FieldScoreOutput `json:"fields"`
	FastTextScores *mat.VecDense      `json:"-"`
	TfidfScores    *mat.VecDense      `json:"-"`
	EntityScores   *mat.VecDense      `json:"-"`
}
