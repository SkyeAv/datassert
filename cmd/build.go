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

const nShards = uint(16)

func selectShard(h uint64) uint {
	asUint := uint(h)
	return asUint % nShards
}

func hashAndShard(str string) (uint64, uint) {
	h := computeXXHash(str)
	shardNum := selectShard(h)
	return h, shardNum
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

	h, shardNum := hashAndShard(key)
	s := &cl.shards[shardNum]
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[h] = strings.Join(val, "\t")
}

func (cl *ClassLookup) Get(h uint64, shardNum uint) ([]string, bool) {
	s := &cl.shards[shardNum]
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
	out := []string{}

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

	bar := uiprogress.AddBar(len(fileNames))
	bar.PrependElapsed()
	bar.PrependFunc(func(b *uiprogress.Bar) string {
		return "Building In-Memory Classes... "
	})
	bar.AppendCompleted()

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
	counter atomic.Uint32
	shards  [nShards]struct {
		m sync.Map
	}
}

func (cm *CategoryMap) GetOrAdd(category string) uint32 {
	h := computeXXHash(category)
	whichShard := selectShard(h)

	s := &cm.shards[whichShard]

	if val, ok := s.m.Load(category); ok {
		return val.(uint32)
	}
	actual, _ := s.m.LoadOrStore(category, cm.counter.Add(1))
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

		tempCategories[i] = categoryShard
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
	_, err := os.Stat(filePath)
	if err != nil {
		err := parquet.WriteFile(filePath, tableShard)
		checkError(7, err)
	}
}

func makeParquetName(datassertDir string, fileName string, thing string, fileNum int, shardNum uint, workerID int) string {
	base := filepath.Base(fileName)
	stem := strings.ToLower(strings.TrimSuffix(base, "Synonyms.ndjson.zst"))

	return fmt.Sprintf("%v/.parquets/%v-%v:%d-%d:%d.parquet", datassertDir, stem, thing, fileNum, shardNum, workerID)
}

func writeIfGtLen[T ParquetTable](datassertDir string, fileName string, thing string, fileNum int, shardNum uint, workerID int, tableShard []T, maxBatch int) (int, []T) {
	if len(tableShard) > maxBatch {
		parquetName := makeParquetName(datassertDir, fileName, thing, fileNum, shardNum, workerID)
		writeParquet(parquetName, tableShard)
		return fileNum + 1, []T{}
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

func (cc *CurieCounter) GetOrNext(h uint64, shardNum uint) uint32 {
	s := &cc.shards[shardNum]

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

func processSynonymRecords(datassertDir string, fileName string, workerID int, records <-chan SynonymRecord, cl *ClassLookup, cm *CategoryMap, cc *CurieCounter) {
	tempCuries := [nShards][]CuriesTable{}
	tempSynonyms := [nShards][]SynonymsTable{}
	tempCategories := [nShards][]CategoriesTable{}

	curieNum := 1
	synonymNum := 1
	categoryNum := 1

	for sr := range records {
		curie := sr.Curie
		if isBadToken(curie) {
			continue
		}

		h, shardNum := hashAndShard(curie)
		curieID := cc.GetOrNext(h, shardNum)

		synonyms := sr.Synonyms
		synonyms = append(synonyms, curie)
		cleaned := cleanAliases(synonyms)

		if aliases, ok := cl.Get(h, shardNum); ok {
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

		curieRow := CuriesTable{
			CurieID:       curieID,
			Curie:         curie,
			PreferredName: preferred,
			CategoryID:    categoryID,
			Taxon:         uint32(taxon),
		}

		categoryRow := CategoriesTable{
			CategoryID: categoryID,
			Category:   category,
		}

		curieWrittenToShard := [nShards]bool{}

		for _, synonym := range l0Synonyms {
			_, termShardNum := hashAndShard(synonym)

			if !curieWrittenToShard[termShardNum] {
				tempCuries[termShardNum] = append(tempCuries[termShardNum], curieRow)
				curieNum, tempCuries[termShardNum] = writeIfGtLen(datassertDir, fileName, "curies", curieNum, termShardNum, workerID, tempCuries[termShardNum], batchSize)

				tempCategories[termShardNum] = append(tempCategories[termShardNum], categoryRow)
				categoryNum, tempCategories[termShardNum] = writeIfGtLen(datassertDir, fileName, "categories", categoryNum, termShardNum, workerID, tempCategories[termShardNum], batchSize)

				curieWrittenToShard[termShardNum] = true
			}

			tempSynonyms[termShardNum] = append(
				tempSynonyms[termShardNum],
				SynonymsTable{
					CurieID:  curieID,
					Synonym:  synonym,
					SourceID: uint8(0),
				},
			)
			synonymNum, tempSynonyms[termShardNum] = writeIfGtLen(datassertDir, fileName, "synonyms", synonymNum, termShardNum, workerID, tempSynonyms[termShardNum], batchSize)
		}

		for _, synonym := range l1Synonyms {
			_, termShardNum := hashAndShard(synonym)

			if !curieWrittenToShard[termShardNum] {
				tempCuries[termShardNum] = append(tempCuries[termShardNum], curieRow)
				curieNum, tempCuries[termShardNum] = writeIfGtLen(datassertDir, fileName, "curies", curieNum, termShardNum, workerID, tempCuries[termShardNum], batchSize)

				tempCategories[termShardNum] = append(tempCategories[termShardNum], categoryRow)
				categoryNum, tempCategories[termShardNum] = writeIfGtLen(datassertDir, fileName, "categories", categoryNum, termShardNum, workerID, tempCategories[termShardNum], batchSize)

				curieWrittenToShard[termShardNum] = true
			}

			tempSynonyms[termShardNum] = append(
				tempSynonyms[termShardNum],
				SynonymsTable{
					CurieID:  curieID,
					Synonym:  synonym,
					SourceID: uint8(1),
				},
			)
			synonymNum, tempSynonyms[termShardNum] = writeIfGtLen(datassertDir, fileName, "synonyms", synonymNum, termShardNum, workerID, tempSynonyms[termShardNum], batchSize)
		}
	}

	for i := range nShards {
		_, _ = writeIfGtLen(datassertDir, fileName, "curies", curieNum, i, workerID, tempCuries[i], 0)
		_, _ = writeIfGtLen(datassertDir, fileName, "synonyms", synonymNum, i, workerID, tempSynonyms[i], 0)
		_, _ = writeIfGtLen(datassertDir, fileName, "categories", categoryNum, i, workerID, tempCategories[i], 0)
	}
}

func parseSynonymFile(datassertDir string, fileName string, nRoutines int, cl *ClassLookup, cm *CategoryMap, cc *CurieCounter, bar *uiprogress.Bar) {
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
			processSynonymRecords(datassertDir, fileName, w, records, cl, cm, cc)
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

func buildSynonymParquets(datassertDir string, fileNames []string, cl *ClassLookup, nRoutines int) {
	cm := CategoryMap{}

	cc := CurieCounter{}
	for i := range nShards {
		cc.shards[i].m = map[uint64]uint32{}
	}

	bar := uiprogress.AddBar(len(fileNames))
	bar.PrependElapsed()
	bar.PrependFunc(func(b *uiprogress.Bar) string {
		return "Building Parquets... "
	})
	bar.AppendCompleted()

	for _, fileName := range fileNames {
		parseSynonymFile(datassertDir, fileName, nRoutines, cl, &cm, &cc, bar)
	}

	for i := range nShards {
		sourceParquet := makeParquetName(datassertDir, "BabelSynonyms.ndjson.zst", "sources", 1, uint(i), 1)
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

func makeDBPath(basePath string, shardNum uint) string {
	return fmt.Sprintf("%v/data/%d.duckdb", basePath, shardNum)
}

func shardGlob(datassertDir string, thing string, shardNum uint) string {
	return fmt.Sprintf("%v/.parquets/*-%v:*-%d:*", datassertDir, thing, shardNum)
}

func buildShardDB(datassertDir string, shardNum uint, bar *uiprogress.Bar) {
	shardPath := makeDBPath(datassertDir, shardNum)
	db := getDB(shardPath)
	defer db.Close()

	iterExecDB(db, dbConfiguration)

	_, err := db.Exec(fmt.Sprintf(
		"CREATE TABLE SOURCES AS SELECT * FROM read_parquet('%v')",
		shardGlob(datassertDir, "sources", shardNum),
	))
	checkError(12, err)

	_, err = db.Exec(fmt.Sprintf(
		"CREATE TABLE CATEGORIES AS SELECT DISTINCT * FROM read_parquet('%v') ORDER BY CATEGORY_NAME",
		shardGlob(datassertDir, "categories", shardNum),
	))
	checkError(13, err)

	_, err = db.Exec(fmt.Sprintf(
		"CREATE TABLE CURIES AS SELECT DISTINCT * FROM read_parquet('%v') ORDER BY TAXON_ID",
		shardGlob(datassertDir, "curies", shardNum),
	))
	checkError(14, err)

	_, err = db.Exec(fmt.Sprintf(
		"CREATE TABLE SYNONYMS AS SELECT * FROM read_parquet('%v') ORDER BY SYNONYM",
		shardGlob(datassertDir, "synonyms", shardNum),
	))
	checkError(15, err)

	iterExecDB(db, indexOps)

	bar.Incr()
}

func buildDuckDBs(datassertDir string) {
	bar := uiprogress.AddBar(int(nShards))
	bar.PrependElapsed()
	bar.PrependFunc(func(b *uiprogress.Bar) string {
		return "Building DuckDB... "
	})
	bar.AppendCompleted()

	for i := range nShards {
		buildShardDB(datassertDir, i, bar)
	}
}

var babelDir string
var dbDir string
var batchSize int
var bufferSize int
var classCPUFraction int
var synonymCPUFraction int

func mkDirs() string {
	datassertDir := filepath.Join(dbDir, "datassert")
	parquetSubDir := filepath.Join(datassertDir, ".parquets")
	dataSubDir := filepath.Join(datassertDir, "data")

	os.RemoveAll(datassertDir)
	os.MkdirAll(parquetSubDir, os.ModePerm)
	os.MkdirAll(dataSubDir, os.ModePerm)

	return datassertDir
}

func build(cmd *cobra.Command, args []string) {
	uiprogress.Start()
	defer uiprogress.Stop()

	datassertDir := mkDirs()

	cpuCount := runtime.NumCPU()

	classFileNames := globFileNames(babelDir, "*Class.ndjson.zst")
	cl := buildClassLookup(classFileNames, (cpuCount / classCPUFraction))

	synonymFileNames := globFileNames(babelDir, "*Synonyms.ndjson.zst")
	buildSynonymParquets(datassertDir, synonymFileNames, cl, (cpuCount / synonymCPUFraction))

	buildDuckDBs(datassertDir)
}

var buildCmd = &cobra.Command{
	Use:   "build --babel-dir <dir>",
	Short: "Build a DuckDB assertion database from Babel exports",
	Long:  "Reads *Class.ndjson.zst and *Synonyms.ndjson.zst files from --babel-dir, writes staging Parquet artifacts to <db-dir>/datassert/.parquets/, and builds 16 sharded DuckDB databases at <db-dir>/datassert/data/{0..15}.duckdb (default --db-dir: current directory).",
	Run:   build,
}

func init() {
	rootCmd.AddCommand(buildCmd)

	buildCmd.Flags().StringVar(&babelDir, "babel-dir", "", "Directory containing Babel *Class.ndjson.zst and *Synonyms.ndjson.zst files.")
	buildCmd.Flags().StringVar(&dbDir, "db-dir", ".", "Base output path for the sharded DuckDB databases.")
	buildCmd.Flags().IntVar(&batchSize, "batch-size", 100000, "Number of records per Parquet batch.")
	buildCmd.Flags().IntVar(&bufferSize, "buffer-size", 2048, "Size of the channel buffer used to process synonym files.")
	buildCmd.Flags().IntVar(&classCPUFraction, "class-cpu-fraction", 2, "Fraction of CPU cores used to ingest Babel *Class.ndjson.zst files.")
	buildCmd.Flags().IntVar(&synonymCPUFraction, "synonym-cpu-fraction", 4, "Fraction of CPU cores used to ingest Babel *Synonyms.ndjson.zst files.")

	buildCmd.MarkFlagRequired("babel-dir")
}
