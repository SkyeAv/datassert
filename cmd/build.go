package cmd

import (
	"fmt"
	"io"
	"log"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

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

func (cl *ClassLookup) Set(key string, value []string) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.data[key] = value
}

func (cl *ClassLookup) Get(key string) ([]string, bool) {
	val, ok := cl.data[key]
	return val, ok
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
	cl := &ClassLookup{data: make(map[string][]string)}
	wg := sync.WaitGroup{}

	for _, fileName := range fileNames {
		wg.Add(1)
		go parseClassFile(fileName, cl, &wg)
	}

	wg.Wait()
	return cl
}

type SynonymRecord struct {
	Curie         string   `json:"curie"`
	Synonyms      []string `json:"names"`
	PreferredName string   `json:"preferred_name"`
	Categories    []string `json:"categories"`
	Taxon         []any    `json:"taxa"`
}

type CategoryMap struct {
	m       sync.Map
	counter atomic.Uint32
}

func (cm *CategoryMap) GetOrAdd(category string) uint32 {
	if val, ok := cm.m.Load(category); ok {
		return val.(uint32)
	}
	actual, _ := cm.m.LoadOrStore(category, cm.counter.Add(1))
	return actual.(uint32)
}

type SourcesTable struct {
	SourceID      uint8  `paruquet:"SOURCE_ID"`
	SourceName    string `paruquet:"SOURCE_NAME"`
	SourceVersion string `paruquet:"SOURCE_VERSION"`
	NLPLevel      uint8  `paruquet:"NLP_LEVEL"`
}

type CategoriesTable struct {
	CategoryID uint32 `paruquet:"CATEGORY_ID"`
	Category   string `paruquet:"CATEGORY"`
}

type SynonymsTable struct {
	CurieID  uint32 `paruquet:"CURIE_ID"`
	SourceID uint8  `paruquet:"SOURCE_ID"`
	Synonym  string `paruquet:"SYNONYM"`
}

type CuriesTable struct {
	CurieID       uint32 `paruquet:"CURIE_ID"`
	Curie         string `paruquet:"CURIE"`
	PreferredName string `paruquet:"PREFERRED_NAME"`
	CategoryID    uint32 `paruquet:"CATEGORY_ID"`
	Taxon         uint32 `paruquet:"TAXON,optional"`
}

func parseSynonymFile(fileName string, cl *ClassLookup, cm *CategoryMap, wg *sync.WaitGroup) {
	defer wg.Done()

	f, err := os.Open(fileName)
	if err != nil {
		throwError(6, err)
	}
	defer f.Close()

	zr, err := zstd.NewReader(f)
	if err != nil {
		throwError(7, err)
	}
	defer zr.Close()

	decoder := sonic.ConfigDefault.NewDecoder(zr)
	for {
		sr := SynonymRecord{}
		err := decoder.Decode(&sr)
		if err == io.EOF {
			break
		} else if err != nil {
			throwError(8, err)
		}

		curie := sr.Curie
		if isBadToken(curie) {
			continue
		}

		synonyms := sr.Synonyms
		synonyms = append(synonyms, curie)
		cleaned := cleanAliases(synonyms)

		if aliases, ok := cl.Get(curie); ok {
			cleaned = append(cleaned, aliases...)
		}

		preferred := sr.PreferredName
		preferred = cleanToken(preferred)

		category := sr.Categories[0]
		categoryID := cm.GetOrAdd(category)

		taxon := 0
		if len(sr.Taxon) > 0 {
			s := fmt.Sprintf("%v", sr.Taxon[0])
			s = strings.TrimPrefix(s, "NCBITaxon:")
			taxon, _ = strconv.Atoi(s)
		}

	}
}

func buildSynonymParquets(fileNames []string, cl *ClassLookup) {
	wg := sync.WaitGroup{}
	cm := CategoryMap{}

	for _, fileName := range fileNames {
		wg.Add(1)
		go parseSynonymFile(fileName, cl, &cm, &wg)
	}
}

func build(cmd *cobra.Command, args []string) {
	babelDir := args[0]

	classFileNames := globFileNames(babelDir, "Class.ndjson.zst")
	cl := buildClassLookup(classFileNames)

	synonymFileNames := globFileNames(babelDir, "Synonym.ndjson.zst")
	buildSynonymParquets(synonymFileNames, cl)
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
