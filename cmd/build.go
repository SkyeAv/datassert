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
	"github.com/parquet-go/parquet-go"
	"github.com/spf13/cobra"
)

func throwError(code uint8, err error) {
	log.Fatalf("%d | ERROR | %v", code, err)
}

func checkError(code uint8, err error) {
	if err != nil {
		throwError(code, err)
	}
}

func globFileNames(dir string, suffix string) []string {
	abs, err := filepath.Abs(dir)
	checkError(1, err)

	pattern := filepath.Join(abs, suffix)
	fileNames, err := filepath.Glob(pattern)
	checkError(2, err)

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

var badPrefixes [2]string = [2]string{
	"INCHIKEY",
	"inchikey",
}

var badTokens [3]string = [3]string{
	"uncharacterized protein",
	"uncharacterized gene",
	"hypothetical protein",
}

func isBadToken(token string) bool {
	if token == "" {
		return true
	}

	for _, badPrefix := range badPrefixes {
		if strings.Contains(token, badPrefix) {
			return true
		}
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
	seen := map[string]struct{}{}

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

func yieldReader(fileName string) *os.File {
	f, err := os.Open(fileName)
	checkError(3, err)
	return f
}

func yieldDecoder(f *os.File) *zstd.Decoder {
	zr, err := zstd.NewReader(f)
	checkError(4, err)
	return zr
}

type ClassRecord struct {
	EquivalentIdentifiers []string `json:"equivalent_identifiers"`
}

func parseClassFile(fileName string, cl *ClassLookup, wg *sync.WaitGroup) {
	defer wg.Done()

	f := yieldReader(fileName)
	defer f.Close()

	zr := yieldDecoder(f)
	defer zr.Close()

	decoder := sonic.ConfigDefault.NewDecoder(zr)
	for {
		cr := ClassRecord{}
		err := decoder.Decode(&cr)
		if err == io.EOF {
			break
		}
		checkError(5, err)

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
	cl := &ClassLookup{data: map[string][]string{}}
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

type CategoriesTable struct {
	CategoryID uint32 `paruquet:"CATEGORY_ID"`
	Category   string `paruquet:"CATEGORY"`
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

func (cm *CategoryMap) ToTable() []CategoriesTable {
	tempCategories := []CategoriesTable{}
	for categoryName, categoryID := range cm.m.Range {
		tempCategories = append(
			tempCategories,
			CategoriesTable{
				CategoryID: categoryID.(uint32),
				Category:   categoryName.(string),
			},
		)
	}

	return tempCategories
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

type SourcesTable struct {
	SourceID      uint8  `paruquet:"SOURCE_ID"`
	SourceName    string `paruquet:"SOURCE_NAME"`
	SourceVersion string `paruquet:"SOURCE_VERSION"`
	NLPLevel      uint8  `paruquet:"NLP_LEVEL"`
}

type ParquetTable interface {
	CuriesTable | SynonymsTable | CategoriesTable | SourcesTable
}

func writeParquet[T ParquetTable](filePath string, table []T) {
	f, err := os.Create(filePath)
	checkError(6, err)
	defer f.Close()

	err = parquet.WriteFile(filePath, table)
	checkError(7, err)
}

var baseDir string = "./.parquet-store/"

func makeParquetName(fileName string, thing string, num int) string {
	base := filepath.Base(fileName)
	stem := strings.TrimSuffix(base, "Synonyms.ndjson.zst")

	return fmt.Sprintf("%v%v%v-%d", baseDir, stem, thing, num)
}

func writeIfGeLen[T ParquetTable](fileName string, thing string, num int, table []T, batchSize int) int {
	if len(table) >= batchSize {
		parquetName := makeParquetName(fileName, thing, num)
		writeParquet(parquetName, table)
		return num + 1
	}
	return num
}

func stringToInt(str string) int {
	num, err := strconv.Atoi(str)
	checkError(8, err)
	return num
}

var l1Regex = regexp.MustCompile(`\W+`)

type CurieCounter struct {
	counter atomic.Uint32
}

func (cc *CurieCounter) Next() uint32 {
	return cc.counter.Add(1) - 1
}

func parseSynonymFile(fileName string, batchSize int, cl *ClassLookup, cm *CategoryMap, cc *CurieCounter, wg *sync.WaitGroup) {
	defer wg.Done()

	f := yieldReader(fileName)
	defer f.Close()

	zr := yieldDecoder(f)
	defer zr.Close()

	tempCuries := []CuriesTable{}
	tempSynonyms := []SynonymsTable{}

	curieNum := 1
	synonymNum := 1

	decoder := sonic.ConfigDefault.NewDecoder(zr)
	for {
		sr := SynonymRecord{}
		err := decoder.Decode(&sr)
		if err == io.EOF {
			break
		}
		checkError(9, err)

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
			l1 := l1Regex.ReplaceAllString(synonym, "")
			l1Synonyms = append(l1Synonyms, l1)
		}

		preferred := sr.PreferredName
		preferred = cleanToken(preferred)

		category := sr.Categories[0]
		categoryID := cm.GetOrAdd(category)

		taxon := 0
		if len(sr.Taxon) > 0 {
			str := fmt.Sprintf("%v", sr.Taxon[0])
			str = strings.TrimPrefix(str, "NCBITaxon:")
			taxon = stringToInt(str)
		}

		tempCuries = append(
			tempCuries,
			CuriesTable{
				CurieID:       curieID,
				Curie:         curie,
				PreferredName: preferred,
				CategoryID:    categoryID,
				Taxon:         uint32(taxon),
			},
		)

		curieNum = writeIfGeLen(fileName, "Curies", curieNum, tempCuries, batchSize)

		newSynonyms := []SynonymsTable{}
		for _, synonym := range l0Synonyms {
			newSynonyms = append(
				newSynonyms,
				SynonymsTable{
					CurieID:  curieID,
					Synonym:  synonym,
					SourceID: uint8(0),
				},
			)
		}
		for _, synonym := range l1Synonyms {
			newSynonyms = append(
				newSynonyms,
				SynonymsTable{
					CurieID:  curieID,
					Synonym:  synonym,
					SourceID: uint8(1),
				},
			)
		}

		tempSynonyms = append(tempSynonyms, newSynonyms...)
		synonymNum = writeIfGeLen(fileName, "Synonyms", synonymNum, tempCuries, batchSize)
	}
}

var sources []SourcesTable = []SourcesTable{
	{
		SourceID:      uint8(0),
		SourceName:    "BABEL",
		SourceVersion: "SEPT-2025",
		NLPLevel:      uint8(0),
	},
	{
		SourceID:      uint8(1),
		SourceName:    "BABEL",
		SourceVersion: "SEPT-2025",
		NLPLevel:      uint8(1),
	},
}

func buildSynonymParquets(fileNames []string, cl *ClassLookup, batchSize int) {
	wg := sync.WaitGroup{}
	cm := CategoryMap{}
	cc := CurieCounter{}

	for _, fileName := range fileNames {
		wg.Add(1)
		go parseSynonymFile(fileName, batchSize, cl, &cm, &cc, &wg)
	}

	categoryParquet := makeParquetName("Biolink", "Categories", 1)
	writeParquet(categoryParquet, cm.ToTable())

	sourceParquet := makeParquetName("BABEL", "Sources", 1)
	writeParquet(sourceParquet, sources)
}

func build(cmd *cobra.Command, args []string) {
	babelDir := args[0]
	batchLen := args[1]

	batchSize := stringToInt(batchLen)

	classFileNames := globFileNames(babelDir, "Class.ndjson.zst")
	cl := buildClassLookup(classFileNames)

	synonymFileNames := globFileNames(babelDir, "Synonyms.ndjson.zst")
	buildSynonymParquets(synonymFileNames, cl, batchSize)
}

var buildCmd = &cobra.Command{
	Use:   "build",
	Short: "Placeholder",
	Long:  "Placeholder",
	Run:   build,
}

func init() {
	os.MkdirAll(baseDir, os.ModePerm)

	rootCmd.AddCommand(buildCmd)
}
