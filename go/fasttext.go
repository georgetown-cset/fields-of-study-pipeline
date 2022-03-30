/*
Create fastText vectors for document text.

We're using a Go wrapper for the fastText C++ library. Unfortunately this prevents static linking.

References:
- https://github.com/taufik-rama/fasttext-go-wrapper
*/
package main

import (
	"gonum.org/v1/gonum/mat"
	"log"
)

// FastTextVectorizer holds a fastText model
type FastTextVectorizer struct {
	Model *Model
}

// NewFastTextVectorizer loads a fastText model from the disk
func NewFastTextVectorizer() *FastTextVectorizer {
	fasttext, err := NewModel(paths.FastTextModel())
	if err != nil {
		log.Fatalf("%v", err)
	}
	return &FastTextVectorizer{fasttext}
}

// Vectorize embeds document text via fastText
func (f *FastTextVectorizer) Vectorize(doc *Doc) *mat.VecDense {
	array, err := f.Model.GetSentenceVector(doc.Text)
	if err != nil {
		log.Fatal(err)
	}
	v := mat.NewVecDense(fastTextDim, array)
	v = NormVector(v)
	return v
}

// NormVector takes the l2 norm of a dense vector
func NormVector(v *mat.VecDense) *mat.VecDense {
	norm := (*v).Norm(2)
	v.ScaleVec(1.0/norm, v)
	return v
}
