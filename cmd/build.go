package cmd

import (
	"database/sql"
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

	"runtime"

	"github.com/bytedance/sonic"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/parquet-go/parquet-go"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
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

var badPrefixes []string = []string{
	"INCHIKEY",
	"inchikey",
}

var badTokens []string = []string{
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

	if slices.Contains(badTokens, token) {
		return true
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

func parseClassFile(fileName string, cl *ClassLookup) {
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

func buildClassLookup(fileNames []string, nRoutines int) *ClassLookup {
	g := &errgroup.Group{}
	g.SetLimit(nRoutines)

	cl := &ClassLookup{data: map[string][]string{}}

	for _, fileName := range fileNames {
		g.Go(func() error {
			parseClassFile(fileName, cl)
			return nil
		})
	}

	g.Wait()
	return cl
}

type SynonymRecord struct {
	Curie         string   `json:"curie"`
	Synonyms      []string `json:"names"`
	PreferredName string   `json:"preferred_name"`
	Categories    []string `json:"types"`
	Taxon         []any    `json:"taxa"`
}

type CategoriesTable struct {
	CategoryID uint32 `parquet:"CATEGORY_ID"`
	Category   string `parquet:"CATEGORY_NAME"`
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
	CurieID  uint32 `parquet:"CURIE_ID"`
	SourceID uint8  `parquet:"SOURCE_ID"`
	Synonym  string `parquet:"SYNONYM"`
}

type CuriesTable struct {
	CurieID       uint32 `parquet:"CURIE_ID"`
	Curie         string `parquet:"CURIE"`
	PreferredName string `parquet:"PREFERRED_NAME"`
	CategoryID    uint32 `parquet:"CATEGORY_ID"`
	Taxon         uint32 `parquet:"TAXON_ID,optional"`
}

type SourcesTable struct {
	SourceID      uint8  `parquet:"SOURCE_ID"`
	SourceName    string `parquet:"SOURCE_NAME"`
	SourceVersion string `parquet:"SOURCE_VERSION"`
	NLPLevel      uint8  `parquet:"NLP_LEVEL"`
}

type ParquetTable interface {
	CuriesTable | SynonymsTable | CategoriesTable | SourcesTable
}

func writeParquet[T ParquetTable](filePath string, table []T) {
	err := parquet.WriteFile(filePath, table)
	checkError(7, err)
}

var parquetBaseDir string = "./.parquet-store/"

func makeParquetName(fileName string, thing string, num int) string {
	base := filepath.Base(fileName)
	stem := strings.TrimSuffix(base, "Synonyms.ndjson.zst")

	return fmt.Sprintf("%v%v%v-%d.parquet", parquetBaseDir, stem, thing, num)
}

func writeIfGtLen[T ParquetTable](fileName string, thing string, num int, table []T, batchSize int) (int, []T) {
	if len(table) > batchSize {
		parquetName := makeParquetName(fileName, thing, num)
		writeParquet(parquetName, table)
		return num + 1, table[:0]
	}
	return num, table
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

func parseSynonymFile(fileName string, batchSize int, cl *ClassLookup, cm *CategoryMap, cc *CurieCounter) {

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

		slices.Sort(cleaned)
		l0Synonyms := slices.Compact(cleaned)
		l0Set := map[string]struct{}{}
		for _, synonym := range l0Synonyms {
			l0Set[synonym] = struct{}{}
		}

		l1Synonyms := []string{}
		for _, synonym := range l0Synonyms {
			l1 := l1Regex.ReplaceAllString(synonym, "")

			if _, exists := l0Set[l1]; !exists {
				l1Synonyms = append(l1Synonyms, l1)
			}
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

		curieNum, tempCuries = writeIfGtLen(fileName, "Curies", curieNum, tempCuries, batchSize)

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
		synonymNum, tempSynonyms = writeIfGtLen(fileName, "Synonyms", synonymNum, tempSynonyms, batchSize)
	}

	_, _ = writeIfGtLen(fileName, "Curies", curieNum, tempCuries, 0)
	_, _ = writeIfGtLen(fileName, "Synonyms", synonymNum, tempSynonyms, 0)
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

func buildSynonymParquets(fileNames []string, cl *ClassLookup, batchSize int, nRoutines int) {
	g := &errgroup.Group{}
	g.SetLimit(nRoutines)

	cm := CategoryMap{}
	cc := CurieCounter{}

	for _, fileName := range fileNames {
		g.Go(func() error {
			parseSynonymFile(fileName, batchSize, cl, &cm, &cc)
			return nil
		})
	}

	g.Wait()

	categoryParquet := makeParquetName("BiolinkSynonyms.ndjson.zst", "Categories", 1)
	writeParquet(categoryParquet, cm.ToTable())

	sourceParquet := makeParquetName("BabelSynonyms.ndjson.zst", "Sources", 1)
	writeParquet(sourceParquet, sources)
}

func getDB(dbPath string) *sql.DB {
	dbPath, err := filepath.Abs(dbPath)
	checkError(6, err)

	db, err := sql.Open("duckdb", dbPath)
	checkError(10, err)
	return db
}

var tableSchemas []string = []string{
	"CREATE TABLE IF NOT EXISTS SOURCES (SOURCE_ID INTEGER PRIMARY KEY, SOURCE_NAME VARCHAR, SOURCE_VERSION VARCHAR, NLP_LEVEL INTEGER);",
	"CREATE TABLE IF NOT EXISTS CATEGORIES (CATEGORY_ID INTEGER PRIMARY KEY, CATEGORY_NAME VARCHAR);",
	"CREATE TABLE IF NOT EXISTS CURIES (CURIE_ID INTEGER PRIMARY KEY, CURIE VARCHAR, PREFERRED_NAME VARCHAR, CATEGORY_ID INTEGER, TAXON_ID INTEGER);",
	"CREATE TABLE IF NOT EXISTS SYNONYMS (CURIE_ID INTEGER, SOURCE_ID INTEGER, SYNONYM VARCHAR);",
	"CREATE TABLE IF NOT EXISTS SYNONYMS_SORTED (CURIE_ID INTEGER, SOURCE_ID INTEGER, SYNONYM VARCHAR);",
}

var dbConfiguration []string = []string{
	fmt.Sprintf("SET temp_directory = '%v'", os.TempDir()),
	"SET preserve_insertion_order = false;",
}

var indexOps []string = []string{
	"INSERT INTO SYNONYMS_SORTED SELECT * FROM SYNONYMS ORDER BY SYNONYM;",
	"DROP TABLE SYNONYMS;",
	"ALTER TABLE SYNONYMS_SORTED RENAME TO SYNONYMS;",
	"CREATE INDEX CURIE_SYNONYMS ON SYNONYMS (SYNONYM);",
	"CREATE INDEX CATEGORY_NAMES ON CATEGORIES (CATEGORY_NAME);",
	"CREATE INDEX CURIE_TAXON ON CURIES (TAXON_ID);",
	"VACUUM ANALYZE;",
}

func iterExecDB(db *sql.DB, queries []string) {
	for _, query := range queries {
		_, err := db.Exec(query)
		checkError(11, err)
	}
}

func iterParquetsDB(db *sql.DB, pattern string, query string) {
	parquets := globFileNames(parquetBaseDir, pattern)
	for _, parquet := range parquets {
		formatted := fmt.Sprintf(query, parquet)
		_, err := db.Exec(formatted)
		checkError(12, err)
	}
}

func buildDuckDB(dbPath string) {
	db := getDB(dbPath)
	defer db.Close()

	iterExecDB(db, tableSchemas)
	iterExecDB(db, dbConfiguration)

	iterParquetsDB(db, "*Sources-*.parquet", "INSERT INTO SOURCES SELECT SOURCE_ID, SOURCE_NAME, SOURCE_VERSION, NLP_LEVEL FROM read_parquet('%v')")
	iterParquetsDB(db, "*Categories-*.parquet", "INSERT INTO CATEGORIES SELECT CATEGORY_ID, CATEGORY_NAME FROM read_parquet('%v')")
	iterParquetsDB(db, "*Curies-*.parquet", "INSERT INTO CURIES SELECT CURIE_ID, CURIE, PREFERRED_NAME, CATEGORY_ID, TAXON_ID FROM read_parquet('%v')")
	iterParquetsDB(db, "*Synonyms-*.parquet", "INSERT INTO SYNONYMS SELECT CURIE_ID, SOURCE_ID, SYNONYM FROM read_parquet('%v')")

	iterExecDB(db, indexOps)
}

var babelDir string
var dbPath string
var batchSize int

func build(cmd *cobra.Command, args []string) {
	cpuCount := runtime.NumCPU()

	classFileNames := globFileNames(babelDir, "*Class.ndjson.zst")
	cl := buildClassLookup(classFileNames, (cpuCount / 2))

	synonymFileNames := globFileNames(babelDir, "*Synonyms.ndjson.zst")
	buildSynonymParquets(synonymFileNames, cl, batchSize, (cpuCount / 4))

	buildDuckDB(dbPath)
}

var buildCmd = &cobra.Command{
	Use:   "build",
	Short: "Build Datassert data from a Babel directory",
	Long:  "Reads Class.ndjson.zst and Synonyms.ndjson.zst from --babel-dir, writes parquet artifacts, and builds a DuckDB database at --db-path (default: ./datassert.duckdb).",
	Run:   build,
}

func init() {
	os.MkdirAll(parquetBaseDir, os.ModePerm)

	rootCmd.AddCommand(buildCmd)

	buildCmd.Flags().StringVar(&babelDir, "babel-dir", "", "Directory containing Babel Class and Synonyms .ndjson.zst files")
	buildCmd.Flags().StringVar(&dbPath, "db-path", "./datassert.duckdb", "Output path for the DuckDB database")
	buildCmd.Flags().IntVar(&batchSize, "batch-size", 1000000, "Number of records per Parquet batch")

	buildCmd.MarkFlagRequired("babel-dir")
}
