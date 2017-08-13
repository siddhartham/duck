package duck

import (
	"encoding/csv"
	"os"
	"sync"
)

// CsvWriter thread safe csv writer
type CsvWriter struct {
	mutex     *sync.Mutex
	csvWriter *csv.Writer
}

// NewCsvWriter returns the CsvWriter
func NewCsvWriter(fileName string) (*CsvWriter, error) {
	csvFile, err := os.Create(fileName)
	if err != nil {
		return nil, err
	}
	w := csv.NewWriter(csvFile)
	return &CsvWriter{csvWriter: w, mutex: &sync.Mutex{}}, nil
}

// Write Write row the CsvWriter
func (w *CsvWriter) Write(row []string) {
	w.mutex.Lock()
	w.csvWriter.Write(row)
	w.mutex.Unlock()
}

// Flush Flush the CsvWriter
func (w *CsvWriter) Flush() {
	w.mutex.Lock()
	w.csvWriter.Flush()
	w.mutex.Unlock()
}
