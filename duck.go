package main

import (
	"encoding/csv"
	"log"
	"os"
	"runtime"
	"sync"

	"github.com/CrowdSurge/banner"
	"github.com/fatih/color"
	duck "github.com/siddhartham/duck/lib"
)

var wg sync.WaitGroup

var cyan = color.New(color.FgCyan)
var italicCyan = cyan.Add(color.Italic)

var yellow = color.New(color.FgHiYellow)
var italicYellow = yellow.Add(color.Italic)

var red = color.New(color.FgRed)
var boldRed = red.Add(color.Bold)

var green = color.New(color.FgGreen)
var boldGreen = green.Add(color.Bold)

var oldCsvPath string
var newCsvPath string
var outCsvPath string
var identifierHead string
var oldHeaders []string
var newHeaders []string
var newHeadersArrangedIndexes []int
var oldIdentifierCol *int
var newIdentifierCol *int

func compareFiles(oldRows [][]string, newRows [][]string, outCsvWriter *duck.CsvWriter) {
	italicCyan.Println("Scaning for changed or new rows...")

	for i, oldRow := range oldRows {
		if i == 0 {
			continue
		}
		wg.Add(1)
		go pickIfDiff(newRows, outCsvWriter, oldRow)
	}
	wg.Add(1)
	go pickNewRows(oldRows, newRows, outCsvWriter)
}

func compareHeaders(outCsvWriter *duck.CsvWriter) {
	italicCyan.Println("Comparing all headers...")
	if len(oldHeaders) != len(newHeaders) {
		log.Fatal("Both the files have different number of headers, hence can not compare!")
	}
	reArrangeNewHeaders()
	if len(oldHeaders) != len(newHeadersArrangedIndexes) {
		log.Fatal("Both the files have different headers, hence can not compare!")
	}
	italicYellow.Println("- found identical")

	outCsvWriter.Write(newHeaders)
}

func chkIdentifier(oldRow []string, newRow []string) {
	italicCyan.Println("Scaning for identifier header in " + oldCsvPath)
	oldHeaders = oldRow
	for i, h := range oldRow {
		if identifierHead == h {
			oldIdentifierCol = &i
			break
		}
	}
	if oldIdentifierCol == nil {
		log.Fatal("Header identifier not found in " + oldCsvPath)
	}

	italicYellow.Println("- found at", *oldIdentifierCol, "th column")

	italicCyan.Println("Scaning for identifier header in " + newCsvPath)
	newHeaders = newRow
	for i, h := range newRow {
		if identifierHead == h {
			newIdentifierCol = &i
			break
		}
	}
	if newIdentifierCol == nil {
		log.Fatal("Header identifier not found in " + newCsvPath)
	}

	italicYellow.Println("- found at", *newIdentifierCol, "th column")
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	args := os.Args[1:]

	banner.Print("  duck  ")
	color.Yellow("Usage: duck [/path/to/old.csv] [path/to/new.csv] [path/to/out.csv] [header identifier]")

	oldCsvPath = args[0]
	newCsvPath = args[1]
	outCsvPath = args[2]
	identifierHead = args[3]

	italicCyan.Println("\n\nComparing " + oldCsvPath + " and " + newCsvPath + "\n")

	oldCsvIn, err := os.Open(oldCsvPath)
	if err != nil {
		log.Fatal(err)
	}
	oldCsvReader := csv.NewReader(oldCsvIn)
	oldRows, err := oldCsvReader.ReadAll()
	if err != nil {
		log.Panic(err)
	}

	newCsvIn, err := os.Open(newCsvPath)
	if err != nil {
		log.Fatal(err)
	}
	newCsvReader := csv.NewReader(newCsvIn)
	newRows, err := newCsvReader.ReadAll()
	if err != nil {
		log.Panic(err)
	}

	outCsvWriter, err := duck.NewCsvWriter(outCsvPath)
	if err != nil {
		log.Panic(err)
	}

	chkIdentifier(oldRows[0], newRows[0])

	compareHeaders(outCsvWriter)

	compareFiles(oldRows, newRows, outCsvWriter)

	wg.Wait()

	outCsvWriter.Flush()
}

func pickNewRows(oldRows [][]string, newRows [][]string, outCsvWriter *duck.CsvWriter) {
	for i, newRow := range newRows {
		var isOld bool
		if i == 0 {
			continue
		}
		for j, oldRow := range oldRows {
			if j == 0 {
				continue
			}
			if oldRow[*oldIdentifierCol] == newRow[*newIdentifierCol] {
				isOld = true
			}
		}
		if isOld != true {
			boldRed.Println(newRow)
			outCsvWriter.Write(newRow)
		}
	}
	wg.Done()
}

func pickIfDiff(newRows [][]string, outCsvWriter *duck.CsvWriter, oldRow []string) {
	var pickRow []string
	for j, newRow := range newRows {
		if j == 0 {
			continue
		}
		if oldRow[*oldIdentifierCol] == newRow[*newIdentifierCol] {
			if isRowChanged(oldRow, newRow) {
				pickRow = newRow
			}
			break
		}
	}

	if pickRow != nil {
		boldGreen.Println(pickRow)
		outCsvWriter.Write(pickRow)
	}
	wg.Done()
}

func reArrangeNewHeaders() {
	newHeaders2 := []int{}
	for _, h1 := range oldHeaders {
		var t2 *int
		for i, h2 := range newHeaders {
			if h1 == h2 {
				t2 = &i
				break
			}
		}
		if t2 != nil {
			newHeaders2 = append(newHeaders2, *t2)
		}
	}
	newHeadersArrangedIndexes = newHeaders2
}

func isRowChanged(oldRow []string, newRow []string) bool {
	var notIdentical bool
	for i, r1 := range oldRow {
		if r1 != newRow[newHeadersArrangedIndexes[i]] {
			notIdentical = true
			break
		}
	}
	return notIdentical
}
