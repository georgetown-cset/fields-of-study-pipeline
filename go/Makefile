BINARY_NAME=fields

all: build test

FASTTEXT_O = $(wildcard fastText-cpp/*.o)

$(FASTTEXT_O):
	mkdir -p fasttext-go-wrapper/fastText/obj
	make -C fastText-cpp

fasttext: $(FASTTEXT_O)
	cp fastText-cpp/*.o fasttext-go-wrapper/fastText/obj
	make -C fasttext-go-wrapper/fastText

build: fasttext
	go build

test:
	go test

run: build
	./${BINARY_NAME} -h

.PHONY: clean
clean:
	go clean
	rm ${BINARY_NAME}
