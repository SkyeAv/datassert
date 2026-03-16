package cmd

import (
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/spf13/cobra"
)

func throwError(code uint8, err error) {
	log.Fatal("%d | ERROR | %v", code, err)
}

func globFileNames(dir string) []string {
	abs, err := filepath.Abs(dir)
	if err != nil {
		throwError(1, err)
	}

	pattern := filepath.Join(abs, "*Class.ndjson.zst")
	fileNames, err := filepath.Glob(pattern)
	if err != nil {
		throwError(2, err)
	}

	return fileNames
}

type ClassLookup struct {
	mu   sync.Mutex
	data map[string][]string
}

func NewClassLookup() *ClassLookup {
	return &ClassLookup{data: make(map[string][]string)}
}

func (cl *ClassLookup) Set(key string, value []string) {
	cl.mu.Lock()
	cl.data[key] = value
	cl.mu.Unlock()
}

type ClassRecord struct {
	EquivalentIdentifiers []string `json:"equivalent_identifiers"`
}

func parseClassFile(fileName string, cl *ClassLookup, wg *sync.WaitGroup) {
	f, err := os.Open(fileName)
	if err != nil {
		throwError(3, err)
	}

	zr, err := zstd.NewReader(f)
	if err != nil {
		throwError(4, err)
	}

	decoder := sonic.ConfigDefault.NewDecoder(zr)

	f.Close()
	zr.Close()
	wg.Done()
}

func buildClassLookup(fileNames []string) *ClassLookup {
	cl := NewClassLookup()
	wg := sync.WaitGroup{}

	for _, fileName := range fileNames {
		wg.Add(1)
		go parseClassFile(fileName, cl, &wg)
	}

	wg.Wait()
	return cl
}

func build(cmd *cobra.Command, args []string) {
	classDir := args[0]
	synonymDir := args[1]

	classFileNames := globFileNames(classDir)
	cl := buildClassLookup(classFileNames)

	synonymFileNames := globFileNames(synonymDir)
}

var buildCmd = &cobra.Command{
	Use:   "build",
	Short: "Placeholder",
	Long:  "Placeholder",
	Run:   build,
}

func init() {
	rootCmd.AddCommand(buildCmd)
}
