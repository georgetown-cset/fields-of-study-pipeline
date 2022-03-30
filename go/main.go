// Define the CLI
// See https://github.com/urfave/cli/blob/master/docs/v2/manual.md
package main

import (
	"github.com/urfave/cli/v2"
	"log"
	"os"
)

const version = "0.0.1"

const fastTextDim = 250
const entityDim = 250

var inputPath string
var outputPath string
var outputPrecision = 4
var outputDelimiter = "\t"
var outputAll bool
var showProgress = false

var maxWorker = 1
var maxQueue = 1

func main() {
	app := &cli.App{
		Name:    "fields",
		Usage:   "CSET implementation of field of study scoring",
		Version: version,
		Commands: []*cli.Command{
			{
				Name:      "score",
				Usage:     "Calculate field scores for document text",
				UsageText: "fields score [options]",
				Action: func(c *cli.Context) error {
					Score()
					return nil
				},
				Before: func(context *cli.Context) error {
					if _, err := os.Stat(paths.Assets); os.IsNotExist(err) {
						log.Fatalf("Assets directory not found: %s", paths.Assets)
					}
					log.Print("Found assets directory")
					return nil
				},
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:        "input",
						Aliases:     []string{"i"},
						Required:    true,
						Usage:       "Path to input JSONL with fields 'merged_id' and 'text'",
						Destination: &inputPath,
					},
					&cli.StringFlag{
						Name:        "output",
						Aliases:     []string{"o"},
						Usage:       "Path to output TSV",
						Destination: &outputPath,
					},
					&cli.StringFlag{
						Name:        "delimit",
						Aliases:     []string{"d"},
						Usage:       "Output delimiter",
						Value:       "\t",
						Destination: &outputDelimiter,
					},
					&cli.IntFlag{
						Name:        "precision",
						Aliases:     []string{"p"},
						Usage:       "Output precision",
						Value:       4,
						Destination: &outputPrecision,
					},
					&cli.IntFlag{
						Name:        "queue",
						Aliases:     []string{"q"},
						Usage:       "Job and result queue buffer size",
						Value:       1,
						Destination: &maxQueue,
					},
					&cli.IntFlag{
						Name:        "workers",
						Aliases:     []string{"w"},
						Usage:       "Worker pool size",
						Value:       1,
						Destination: &maxWorker,
					},
					&cli.BoolFlag{
						Name:        "all",
						Usage:       "Include all scores (intermediate and final) in output",
						Value:       false,
						Destination: &outputAll,
					},
					&cli.StringFlag{
						Name:        "assets",
						Aliases:     []string{"a"},
						Usage:       "Path of assets directory",
						Value:       "./assets",
						Destination: &paths.Assets,
					},
					&cli.StringFlag{
						Name:        "lang",
						Aliases:     []string{"l"},
						Usage:       "Language ('en' or 'zh')",
						Value:       "en",
						Destination: &paths.Lang,
					},
					&cli.BoolFlag{
						Name:        "progress",
						Usage:       "Show progress bar",
						Value:       false,
						Destination: &showProgress,
					},
				},
			},
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
