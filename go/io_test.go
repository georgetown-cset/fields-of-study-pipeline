package main

import (
	"testing"
)

func TestReadInputs(t *testing.T) {
	inputs := ReadInputs("test/short_en_inputs.jsonl")
	var lines [][]byte
	for x := range inputs {
		lines = append(lines, x)
	}
	if len(lines) != 4 {
		t.Logf("Expected 4 lines from short_en_inputs.jsonl but got %d", len(lines))
		t.Fail()
	}
	expected := []string{
		"{\"merged_id\":  \"0\", \"text\":  \"natural-language understanding (nlu) or natural-language interpretation (nli)[1] is a subtopic of natural-language processing in artificial intelligence that deals with machine reading comprehension. natural-language understanding is considered an ai-hard problem.[2]\"}",
		"{\"merged_id\":  \"1\", \"text\":  \"the program student, written in 1964 by daniel bobrow for his phd dissertation at mit, is one of the earliest known attempts at natural-language understanding by a computer.[6][7][8][9][10] eight years after john mccarthy coined the term artificial intelligence, bobrow's dissertation (titled natural language input for a computer problem solving system) showed how a computer could understand simple natural language input to solve algebra word problems.\"}",
		"{\"merged_id\":  \"2\", \"text\":  \"a year later, in 1965, joseph weizenbaum at mit wrote eliza, an interactive program that carried on a dialogue in english on any topic, the most popular being psychotherapy. eliza worked by simple parsing and substitution of key words into canned phrases and weizenbaum sidestepped the problem of giving the program a database of real-world knowledge or a rich lexicon. yet eliza gained surprising popularity as a toy project and can be seen as a very early precursor to current commercial systems such as those used by ask.com.[11]\"}",
		"{\"merged_id\":  \"3\", \"text\":  \"in 1969 roger schank at stanford university introduced the conceptual dependency theory for natural-language understanding.[12] this model, partially influenced by the work of sydney lamb, was extensively used by schank's students at yale university, such as robert wilensky, wendy lehnert, and janet kolodner.\"}",
	}
	for i, line := range lines {
		text := string(line)
		if expected[i] != text {
			t.Log("Expected:", expected[i])
			t.Log("Actual:  ", text)
			t.Fail()
		}
	}
}

func TestReadInputsBlankLinesGzip(t *testing.T) {
	inputs := ReadInputs("test/empty_lines.txt.gz")
	expected := []string{"1", "2", "3", "4", "5"}
	i := 0
	for x := range inputs {
		if string(x) != expected[i] {
			t.Log(string(x), "!=", expected[i])
			t.Fail()
		}
		i++
	}
	if i != 5 {
		t.Log("Expected 5 non-empty lines but got", i)
		t.Fail()
	}
}
func TestIterInputsBlankLines(t *testing.T) {
	inputs := ReadInputs("test/empty_lines.txt")
	expected := []string{"1", "2", "3", "4", "5"}
	i := 0
	for x := range inputs {
		if string(x) != expected[i] {
			t.Log(string(x), "!=", expected[i])
			t.Fail()
		}
		i++
	}
	if i != len(expected) {
		t.Logf("Expected %d non-empty lines but got %d", len(expected), i)
		t.Fail()
	}
}

func TestReadInputsGlob(t *testing.T) {
	inputs := ReadInputs("test/glob-prefix-*.txt")
	expected := []string{"1", "2"}
	i := 0
	for x := range inputs {
		if string(x) != expected[i] {
			t.Log(string(x), "!=", expected[i])
			t.Fail()
		}
		i++
	}
	if i != len(expected) {
		t.Logf("Expected %d non-empty lines but got %d", len(expected), i)
		t.Fail()
	}
}

func TestReadInputsGlobGz(t *testing.T) {
	inputs := ReadInputs("test/glob-prefix-*.txt.gz")
	expected := []string{"1", "2"}
	i := 0
	for x := range inputs {
		if string(x) != expected[i] {
			t.Log(string(x), "!=", expected[i])
			t.Fail()
		}
		i++
	}
	if i != len(expected) {
		t.Logf("Expected %d non-empty lines but got %d", len(expected), i)
		t.Fail()
	}
}
