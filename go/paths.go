/*
Define paths to assets used in scoring.

These are typically in a single directory, and vary
*/
package main

import "path/filepath"

// Paths gives the paths to assets used in scoring
type Paths struct {
	// Assets gives the path to a directory holding asset files
	Assets string
	// Lang gives a lowercase language prefix {"en", "zh"}
	Lang string
}

// Vocab gives the path to tf-idf vocab for vectorizer construction
func (p *Paths) Vocab() string {
	return filepath.Join(p.Assets, p.Lang+"_vocab.txt")
}

// FieldMeta gives the path to field metadata
func (p *Paths) FieldMeta() string {
	return filepath.Join(p.Assets, "fields.tsv")
}

// FieldKey gives the path to a text file containing field IDs in vector matrices' row order
func (p *Paths) FieldKey() string {
	return filepath.Join(p.Assets, p.Lang+"_field_keys.txt")
}

// FieldTfidfVectors gives the path to tf-idf vectors for fields
func (p *Paths) FieldTfidfVectors() string {
	return filepath.Join(p.Assets, p.Lang+"_field_tfidf_vectors.json")
}

// FieldFastTextVectors gives the path to FastText vectors for fields
func (p *Paths) FieldFastTextVectors() string {
	return filepath.Join(p.Assets, p.Lang+"_field_fasttext_vectors.csv")
}

// FieldEntityVectors gives the path to entity FastText vectors for fields
func (p *Paths) FieldEntityVectors() string {
	return filepath.Join(p.Assets, p.Lang+"_field_entity_vectors.csv")
}

// EntityTrie gives the path to the entity trie, which maps key-phrases to fields with entity vectors
func (p *Paths) EntityTrie() string {
	return filepath.Join(p.Assets, p.Lang+"_entity_trie.csv")
}

// FastTextModel gives the path to the FastText model binary
func (p *Paths) FastTextModel() string {
	return filepath.Join(p.Assets, p.Lang+"_fasttext.bin")
}

var paths = &Paths{"./assets", "en"}
