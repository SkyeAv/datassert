package cmd

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/bytedance/sonic"
	"github.com/cespare/xxhash/v2"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/gosuri/uiprogress"
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

func computeXXHash(str string) uint64 {
	b := []byte(str)
	return xxhash.Sum64(b)
}

const nShards = uint(20)

func selectShard(h uint64) uint {
	asUint := uint(h)
	return asUint % nShards
}

func hashAndShard(str string) (uint64, uint) {
	h := computeXXHash(str)
	whichShard := selectShard(h)
	return h, whichShard
}

type ClassLookup struct {
	shards [nShards]struct {
		mu   sync.Mutex
		data map[uint64]string
		_pad [40]byte
	}
}

func (cl *ClassLookup) Set(key string, val []string) {
	if len(val) == 0 {
		return
	}

	h, whichShard := hashAndShard(key)
	s := &cl.shards[whichShard]
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[h] = strings.Join(val, "\t")
}

func (cl *ClassLookup) Get(h uint64, whichShard uint) ([]string, bool) {
	s := &cl.shards[whichShard]
	s.mu.Lock()
	defer s.mu.Unlock()

	val, ok := s.data[h]
	delete(s.data, h)

	split := strings.Split(val, "\t")
	return split, ok
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
	out := aliases[:0]

	for _, a := range aliases {
		c := strings.ToLower(a)
		c = cleanToken(c)

		if !isBadToken(c) {
			out = append(out, c)
		}
	}

	slices.Sort(out)
	return slices.Compact(out)
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

func parseClassFile(fileName string, cl *ClassLookup, bar *uiprogress.Bar) {
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

	bar.Incr()
}

func buildClassLookup(fileNames []string, nRoutines int) *ClassLookup {
	g := &errgroup.Group{}
	g.SetLimit(nRoutines)

	cl := &ClassLookup{}
	for i := range cl.shards {
		cl.shards[i].data = map[uint64]string{}
	}
	n := len(fileNames)
	bar := uiprogress.AddBar(n)
	bar.AppendCompleted()
	bar.PrependElapsed()

	for _, fileName := range fileNames {
		g.Go(func() error {
			parseClassFile(fileName, cl, bar)
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
	shards [nShards]struct {
		m       sync.Map
		counter atomic.Uint32
	}
}

func (cm *CategoryMap) GetOrAdd(category string) uint32 {
	h := computeXXHash(category)
	whichShard := selectShard(h)

	s := &cm.shards[whichShard]

	if val, ok := s.m.Load(category); ok {
		return val.(uint32)
	}
	actual, _ := s.m.LoadOrStore(category, s.counter.Add(1))
	return actual.(uint32)
}

func (cm *CategoryMap) ToTables() [nShards][]CategoriesTable {
	tempCategories := [nShards][]CategoriesTable{}

	for i := range nShards {
		categoryShard := []CategoriesTable{}

		s := &cm.shards[i]
		for categoryName, categoryID := range s.m.Range {
			categoryShard = append(
				categoryShard,
				CategoriesTable{
					CategoryID: categoryID.(uint32),
					Category:   categoryName.(string),
				},
			)
		}

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

func writeParquet[T ParquetTable](filePath string, tableShard []T) {
	err := parquet.WriteFile(filePath, tableShard)
	checkError(7, err)
}

var parquetBaseDir string = "./.parquet-store/"

func makeParquetName(fileName string, thing string, fileNum int, shardNum uint, workerID int) string {
	base := filepath.Base(fileName)
	stem := strings.TrimSuffix(base, "Synonyms.ndjson.zst")

	return fmt.Sprintf("%v%v%v%d-%d-%d.parquet", parquetBaseDir, stem, thing, fileNum, shardNum, workerID)
}

func writeIfGtLen[T ParquetTable](fileName string, thing string, fileNum int, shardNum uint, workerID int, tableShard []T, maxBatch int) (int, []T) {
	if len(tableShard) > maxBatch {
		parquetName := makeParquetName(fileName, thing, fileNum, shardNum, workerID)
		writeParquet(parquetName, tableShard)
		return fileNum + 1, tableShard
	}

	return fileNum, tableShard
}

func stringToInt(str string) int {
	num, err := strconv.Atoi(str)
	checkError(8, err)
	return num
}

var l1Regex = regexp.MustCompile(`\W+`)

type CurieCounter struct {
	counter atomic.Uint32
	shards  [nShards]struct {
		mu   sync.RWMutex
		m    map[uint64]uint32
		_pad [40]byte
	}
}

func (cc *CurieCounter) GetOrNext(h uint64, whichShard uint) uint32 {
	s := &cc.shards[whichShard]

	s.mu.RLock()
	if curieID, ok := s.m[h]; ok {
		s.mu.RUnlock()
		return curieID
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	if curieID, ok := s.m[h]; ok {
		return curieID
	}

	curieID := cc.counter.Add(1) - 1
	s.m[h] = curieID
	return curieID
}

func decodeRecord(records chan SynonymRecord, zr *zstd.Decoder) {
	defer close(records)
	decoder := sonic.ConfigDefault.NewDecoder(zr)
	for {
		var sr SynonymRecord
		err := decoder.Decode(&sr)
		if err == io.EOF {
			return
		}
		checkError(9, err)
		records <- sr
	}
}

func processSynonymRecords(fileName string, workerID int, records <-chan SynonymRecord, cl *ClassLookup, cm *CategoryMap, cc *CurieCounter) {
	tempCuries := [nShards][]CuriesTable{}
	tempSynonyms := [nShards][]SynonymsTable{}

	curieNum := 1
	synonymNum := 1

	for sr := range records {
		curie := sr.Curie
		if isBadToken(curie) {
			continue
		}

		h, whichShard := hashAndShard(curie)
		curieID := cc.GetOrNext(h, whichShard)

		synonyms := sr.Synonyms
		synonyms = append(synonyms, curie)
		cleaned := cleanAliases(synonyms)

		if aliases, ok := cl.Get(h, whichShard); ok {
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

		curieShard := tempCuries[whichShard]
		curieShard = append(
			curieShard,
			CuriesTable{
				CurieID:       curieID,
				Curie:         curie,
				PreferredName: preferred,
				CategoryID:    categoryID,
				Taxon:         uint32(taxon),
			},
		)

		curieNum, curieShard = writeIfGtLen(fileName, "Curies", curieNum, whichShard, workerID, curieShard, batchSize)

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

		synonymsShard := tempSynonyms[whichShard]
		synonymsShard = append(synonymsShard, newSynonyms...)
		synonymNum, synonymsShard = writeIfGtLen(fileName, "Synonyms", synonymNum, whichShard, workerID, synonymsShard, batchSize)
	}

	for i := range nShards {
		_, _ = writeIfGtLen(fileName, "Curies", curieNum, i, workerID, tempCuries[i], 0)
		_, _ = writeIfGtLen(fileName, "Synonyms", synonymNum, i, workerID, tempSynonyms[i], 0)
	}

}

func parseSynonymFile(fileName string, nRoutines int, cl *ClassLookup, cm *CategoryMap, cc *CurieCounter, bar *uiprogress.Bar) {
	f := yieldReader(fileName)
	defer f.Close()

	zr := yieldDecoder(f)
	defer zr.Close()

	records := make(chan SynonymRecord, bufferSize)
	go decodeRecord(records, zr)

	g := &errgroup.Group{}
	g.SetLimit(nRoutines)
	for w := range nRoutines {
		g.Go(func() error {
			processSynonymRecords(fileName, w, records, cl, cm, cc)
			return nil
		})
	}

	g.Wait()
	bar.Incr()
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

func buildSynonymParquets(fileNames []string, cl *ClassLookup, nRoutines int) {
	cm := CategoryMap{}

	cc := CurieCounter{}
	for i := range nShards {
		cc.shards[i].m = map[uint64]uint32{}
	}

	n := len(fileNames)
	bar := uiprogress.AddBar(n)
	bar.AppendCompleted()
	bar.PrependElapsed()

	for _, fileName := range fileNames {
		parseSynonymFile(fileName, nRoutines, cl, &cm, &cc, bar)
	}

	for i, table := range cm.ToTables() {
		categoryParquet := makeParquetName("BiolinkSynonyms.ndjson.zst", "Categories", 1, uint(i), 1)
		writeParquet(categoryParquet, table)
	}

	for i := range nShards {
		sourceParquet := makeParquetName("BabelSynonyms.ndjson.zst", "Sources", 1, uint(i), 1)
		writeParquet(sourceParquet, sources)
	}
}

func getDB(dbPath string) *sql.DB {
	dbPath, err := filepath.Abs(dbPath)
	checkError(6, err)

	db, err := sql.Open("duckdb", dbPath)
	checkError(10, err)
	return db
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

func buildDuckDB(dbPath string) {
	db := getDB(dbPath)
	defer db.Close()

	bar := uiprogress.AddBar(6)
	bar.AppendCompleted()
	bar.PrependElapsed()

	iterExecDB(db, dbConfiguration)
	bar.Incr()

	_, err := db.Exec("CREATE TABLE SOURCES AS SELECT * FROM read_parquet('.parquet-store/*Sources-*.parquet')")
	checkError(12, err)
	bar.Incr()

	_, err = db.Exec("CREATE TABLE CATEGORIES AS SELECT * FROM read_parquet('.parquet-store/*Categories-*.parquet') ORDER BY CATEGORY_NAME")
	checkError(13, err)
	bar.Incr()

	_, err = db.Exec("CREATE TABLE CURIES AS SELECT * FROM read_parquet('.parquet-store/*Curies-*.parquet') ORDER BY TAXON_ID")
	checkError(14, err)

	_, err = db.Exec("CREATE TABLE SYNONYMS AS SELECT * FROM read_parquet('.parquet-store/*Synonyms-*.parquet') ORDER BY SYNONYM")
	checkError(15, err)
	bar.Incr()

	iterExecDB(db, indexOps)
	bar.Incr()
}

func mkDotFiles() {
	totalDirs := int(nShards + 1)
	bar := uiprogress.AddBar(totalDirs)
	bar.AppendCompleted()
	bar.PrependElapsed()

	os.MkdirAll(parquetBaseDir, os.ModePerm)
	bar.Incr()

	for i := range nShards {
		asStr := fmt.Sprintf("%d", i)
		parquetShard := filepath.Join(parquetBaseDir, asStr)

		os.MkdirAll(parquetShard, os.ModePerm)
		bar.Incr()
	}
}

var babelDir string
var dbPath string
var batchSize int
var bufferSize int
var classCPUFraction int
var synonymCPUFraction int

func build(cmd *cobra.Command, args []string) {
	uiprogress.Start()
	defer uiprogress.Stop()

	mkDotFiles()

	cpuCount := runtime.NumCPU()

	classFileNames := globFileNames(babelDir, "*Class.ndjson.zst")
	cl := buildClassLookup(classFileNames, (cpuCount / classCPUFraction))

	synonymFileNames := globFileNames(babelDir, "*Synonyms.ndjson.zst")
	buildSynonymParquets(synonymFileNames, cl, (cpuCount / synonymCPUFraction))

	buildDuckDB(dbPath)
}

var buildCmd = &cobra.Command{
	Use:   "build --babel-dir <dir>",
	Short: "Build a DuckDB assertion database from Babel exports",
	Long:  "Reads *Class.ndjson.zst and *Synonyms.ndjson.zst files from --babel-dir, writes staging parquet artifacts to ./.parquet-store/, and builds a DuckDB database at --db-path (default: ./datassert.duckdb).",
	Run:   build,
}

func init() {
	rootCmd.AddCommand(buildCmd)

	buildCmd.Flags().StringVar(&babelDir, "babel-dir", "", "Directory containing Babel *Class.ndjson.zst and *Synonyms.ndjson.zst files.")
	buildCmd.Flags().StringVar(&dbPath, "db-path", "./.datassert", "Output path for the DuckDB databases.")
	buildCmd.Flags().IntVar(&batchSize, "batch-size", 1000000, "Number of records per Parquet batch.")
	buildCmd.Flags().IntVar(&bufferSize, "buffer-size", 2048, "Size of the channel buffer used to process synonym files.")
	buildCmd.Flags().IntVar(&classCPUFraction, "class-cpu-fraction", 2, "Fraction of CPU cores used to ingest Babel *Class.ndjson.zst files.")
	buildCmd.Flags().IntVar(&classCPUFraction, "synonym-cpu-fraction", 4, "Fraction of CPU cores used to ingest Babel *Class.ndjson.zst files.")

	buildCmd.MarkFlagRequired("babel-dir")
}
