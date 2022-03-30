/*
Define vectorizers.
*/
package main

import (
	"gonum.org/v1/gonum/mat"
)

// Vectors holds the three vectorizers we use to embed a Doc.
// We always use them together, so having them in the same place and loading them together is convenient.
type Vectors struct {
	FastText *FastTextVectorizer
	Tfidf    *TfidfVectorizer
	Entity   *EntityVectorizer
}

// DocEmbedding holds the result of applying vectorizers to document text.
type DocEmbedding struct {
	MergedId string
	FastText *mat.VecDense
	Entity   *mat.VecDense
	Tfidf    *[]Tfidf
}

// NewVectors loads vectors from the disk.
func NewVectors() *Vectors {
	v := Vectors{
		NewFastTextVectorizer(),
		NewTfidfVectorizer(),
		NewEntityVectorizer(),
	}
	return &v
}

// Embed applies the vectorizers to a Doc yielding a DocEmbedding.
func (v *Vectors) Embed(doc *Doc) DocEmbedding {
	embedding := DocEmbedding{
		doc.MergedId,
		v.FastText.Vectorize(doc),
		v.Entity.Vectorize(doc),
		v.Tfidf.Vectorize(doc.Text),
	}
	return embedding
}
