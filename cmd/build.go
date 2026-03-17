package cmd

import (
	"io"
	"log"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/bytedance/sonic"
	"github.com/klauspost/compress/zstd"
	"github.com/spf13/cobra"
)

func throwError(code uint8, err error) {
	log.Fatal("%d | ERROR | %v", code, err)
}

func globFileNames(dir string, suffix string) []string {
	abs, err := filepath.Abs(dir)
	if err != nil {
		throwError(1, err)
	}

	pattern := filepath.Join(abs, suffix)
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
	defer cl.mu.Unlock()
	cl.data[key] = value
}

type ClassRecord struct {
	EquivalentIdentifiers []string `json:"equivalent_identifiers"`
}

var badTokens = [2]string{
	"uncharacterized protein",
	"hypothetical protein",
}

func isBadToken(token string) bool {
	if token == "" {
		return true
	}

	if strings.Contains(token, "INCHIKEY") || strings.Contains(token, "inchikey") {
		return true
	}

	for _, badToken := range badTokens {
		if token == badToken {
			return true
		}
	}

	return false
}

func cleanToken(token string) string {
	token = strings.TrimSpace(token)
	for len(token) >= 2 {
		wrapped := token[0] == token[len(token)-1] && (token[0] == '\'' || token[0] == '"')
		doublePrefix := token[:2] == "''" || token[:2] == `""`

		if wrapped {
			token = strings.TrimSpace(token[1 : len(token)-1])
		} else if doublePrefix {
			token = strings.TrimSpace(token[2:])
		} else {
			break
		}
	}

	return token
}

func cleanAliases(aliases []string) []string {
	seen := make(map[string]struct{})

	for _, a := range aliases {
		c := strings.ToLower(a)
		c = cleanToken(c)

		if !isBadToken(c) {
			seen[c] = struct{}{}
		}
	}

	out := maps.Keys(seen)
	return slices.Collect(out)
}

func parseClassFile(fileName string, cl *ClassLookup, wg *sync.WaitGroup) {
	defer wg.Done()

	f, err := os.Open(fileName)
	if err != nil {
		throwError(3, err)
	}
	defer f.Close()

	zr, err := zstd.NewReader(f)
	if err != nil {
		throwError(4, err)
	}
	defer zr.Close()

	decoder := sonic.ConfigDefault.NewDecoder(zr)
	for {
		cr := ClassRecord{}
		err := decoder.Decode(&cr)
		if err == io.EOF {
			break
		} else if err != nil {
			throwError(5, err)
		}

		ids := cr.EquivalentIdentifiers
		if len(ids) == 0 {
			continue
		}

		curie, aliases := ids[0], ids[1:]
		if isBadToken(curie) {
			continue
		}

		aliases = cleanAliases(aliases)
		cl.Set(curie, aliases)
	}
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

var categories = sync.Map{}

func buildSynonymParquets(fileNames []string, cl *ClassLookup) []string {
	return []string{}
}

func build(cmd *cobra.Command, args []string) {
	babelDir := args[0]

	classFileNames := globFileNames(babelDir, "Class.ndjson.zst")
	cl := buildClassLookup(classFileNames)

	synonymFileNames := globFileNames(babelDir, "Synonym.ndjson.zst")
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
