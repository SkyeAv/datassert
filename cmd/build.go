package cmd

import (
	"fmt"
	"io"
	"log"
	"maps"
	"os"
	"path/filepath"
	"regexp"
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
	log.Fatalf("%d | ERROR | %v", code, err)
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

var l1Regex = regexp.MustCompile(`\W+`)

type CurieCounter struct {
	counter atomic.Uint32
}

func (cc *CurieCounter) Next() uint32 {
	return cc.counter.Add(1) - 1
}

func parseSynonymFile(fileName string, batchSize uint32, cl *ClassLookup, cm *CategoryMap, cc *CurieCounter, wg *sync.WaitGroup) {
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

	syt := []SynonymsTable{}
	cut := []CuriesTable{}

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
		curieID := cc.Next()

		synonyms := sr.Synonyms
		synonyms = append(synonyms, curie)
		cleaned := cleanAliases(synonyms)

		if aliases, ok := cl.Get(curie); ok {
			cleaned = append(cleaned, aliases...)
		}

		l0Synonyms := slices.Compact(cleaned)
		l1Synonyms := []string{}
		for _, synonym := range l0Synonyms {
			l1Synonyms = append(l1Synonyms, l1Regex.ReplaceAllString(synonym, ""))
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

		taxonID := uint32(taxon)
		cut = append(cut, CuriesTable{CurieID: curieID, Curie: curie, PreferredName: preferred, CategoryID: categoryID, Taxon: taxonID})

		newSynonyms := []SynonymsTable{}
		for _, synonym := range l0Synonyms {
			newSynonyms = append(newSynonyms, SynonymsTable{CurieID: curieID, Synonym: synonym, SourceID: 0})
		}
		for _, synonym := range l1Synonyms {
			newSynonyms = append(newSynonyms, SynonymsTable{CurieID: curieID, Synonym: synonym, SourceID: 1})
		}
		syt = append(syt, newSynonyms...)
	}
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

func buildSynonymParquets(fileNames []string, cl *ClassLookup, batchSize uint32) {
	wg := sync.WaitGroup{}
	cm := CategoryMap{}
	cc := CurieCounter{}

	for _, fileName := range fileNames {
		wg.Add(1)
		go parseSynonymFile(fileName, batchSize, cl, &cm, &cc, &wg)
	}
}

func build(cmd *cobra.Command, args []string) {
	babelDir := args[0]
	batchLen := args[1]

	batchInt, err := strconv.Atoi(batchLen)
	if err != nil {
		throwError(9, err)
	}

	batchSize := uint32(batchInt)

	classFileNames := globFileNames(babelDir, "Class.ndjson.zst")
	cl := buildClassLookup(classFileNames)

	synonymFileNames := globFileNames(babelDir, "Synonym.ndjson.zst")
	buildSynonymParquets(synonymFileNames, cl, batchSize)
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
