package cmd

import (
	"bufio"
	"compress/gzip"
	"database/sql"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"
	"github.com/cespare/xxhash/v2"
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/gosuri/uiprogress"
	"github.com/parquet-go/parquet-go"
	"github.com/pierrec/lz4/v4"
	"github.com/puzpuzpuz/xsync/v3"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

var configuration []string = []string{
	fmt.Sprintf("SET temp_directory = '%v'", os.TempDir()),
	"SET preserve_insertion_order = false;",
}

var indexes []string = []string{
	"CREATE INDEX CURIE_SYNONYMS ON SYNONYMS (SYNONYM);",
	"CREATE INDEX CATEGORY_NAMES ON CATEGORIES (CATEGORY_NAME);",
	"CREATE INDEX CURIE_TAXON ON CURIES (TAXON_ID);",
	"VACUUM ANALYZE;",
}

func generateDuckDBs() {
	bar := uiprogress.AddBar(int(shards))
	bar.PrependElapsed()
	bar.PrependFunc(func(b *uiprogress.Bar) string {
		return "Generating DuckDB Databases:"
	})
	bar.AppendCompleted()

	for shard := range shards {
		s := fmt.Sprintf("%v", shard)
		duckDBPath := fmt.Sprintf("%v/%v.duckdb", data, s)
		shardPath := fmt.Sprintf("%v/%v", parquets, s)

		db, err := sql.Open("duckdb", duckDBPath)
		if err != nil {
			log.Fatal(err)
		}

		for _, instruction := range configuration {
			_, err := db.Exec(instruction)
			if err != nil {
				log.Fatal(err)
			}
		}

		sourcesQuery := fmt.Sprintf("CREATE TABLE SOURCES AS SELECT * FROM read_parquet('%v/*.sources.parquet');", shardPath)
		_, err = db.Exec(sourcesQuery)
		if err != nil {
			log.Fatal(err)
		}

		categoriesQuery := fmt.Sprintf("CREATE TABLE CATEGORIES AS SELECT * FROM read_parquet('%v/*.categories.parquet') ORDER BY CATEGORY_NAME;", shardPath)
		_, err = db.Exec(categoriesQuery)
		if err != nil {
			log.Fatal(err)
		}

		curiesQuery := fmt.Sprintf("CREATE TABLE CURIES AS SELECT DISTINCT ON (CURIE_ID) * FROM read_parquet('%v/*.curies.parquet') ORDER BY CURIE, CURIE_ID ASC, TAXON_ID;", shardPath)
		_, err = db.Exec(curiesQuery)
		if err != nil {
			log.Fatal(err)
		}

		synonymsQuery := fmt.Sprintf("CREATE TABLE SYNONYMS AS SELECT DISTINCT ON (SYNONYM, CURIE_ID) * FROM read_parquet('%v/*.synonyms.parquet') ORDER BY SYNONYM, CURIE_ID ASC, SOURCE_ID;", shardPath)
		_, err = db.Exec(synonymsQuery)
		if err != nil {
			log.Fatal(err)
		}

		for _, index := range indexes {
			_, err := db.Exec(index)
			if err != nil {
				log.Fatal(err)
			}
		}

		bar.Incr()
	}
}

type curieCounter struct {
	counter atomic.Uint32
	shard   [shards]struct {
		mu sync.RWMutex
		id map[uint64]uint32
		// structure this so that total bytes is 64
		pad [32]byte
	}
}

func (u *curieCounter) GetCount(shard uint, h uint64) uint32 {
	s := &u.shard[shard]

	s.mu.RLock()
	if id, ok := s.id[h]; ok {
		s.mu.RUnlock()
		return id
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	if id, ok := s.id[h]; ok {
		return id
	}

	id := u.counter.Add(1)
	s.id[h] = id
	return id
}

type categoriesSchema struct {
	CategoryID uint32 `parquet:"CATEGORY_ID"`
	Category   string `parquet:"CATEGORY_NAME"`
}

type categoryCounter struct {
	counter    atomic.Uint32
	categories *xsync.MapOf[string, uint32]
}

func (c *categoryCounter) GetCount(shard uint, category string) uint32 {
	if val, ok := c.categories.Load(category); ok {
		return val
	}

	val, _ := c.categories.LoadOrStore(category, c.counter.Add(1))
	return val
}

func (c *categoryCounter) BuildSchema() []categoriesSchema {
	var schema []categoriesSchema = []categoriesSchema{}

	for name, id := range c.categories.Range {
		schema = append(schema, categoriesSchema{Category: name, CategoryID: id})
	}

	return schema
}

type synonymsJSON struct {
	Curie         string   `json:"curie"`
	Synonyms      []string `json:"names"`
	PreferredName string   `json:"preferred_name"`
	Categories    []string `json:"types"`
	Taxon         []any    `json:"taxa"`
}

type synonymsSchema struct {
	CurieID  uint32 `parquet:"CURIE_ID"`
	SourceID uint8  `parquet:"SOURCE_ID"`
	Synonym  string `parquet:"SYNONYM"`
}

type curiesSchema struct {
	CurieID       uint32 `parquet:"CURIE_ID"`
	Curie         string `parquet:"CURIE"`
	PreferredName string `parquet:"PREFERRED_NAME"`
	CategoryID    uint32 `parquet:"CATEGORY_ID"`
	Taxon         uint32 `parquet:"TAXON_ID,optional"`
}

type sourcesSchema struct {
	SourceID      uint8  `parquet:"SOURCE_ID"`
	SourceName    string `parquet:"SOURCE_NAME"`
	SourceVersion string `parquet:"SOURCE_VERSION"`
	NLPLevel      uint8  `parquet:"NLP_LEVEL"`
}

var hardcodedSources []sourcesSchema = []sourcesSchema{
	{SourceID: 1, SourceName: "BABEL", SourceVersion: version, NLPLevel: 1},
	{SourceID: 2, SourceName: "BABEL", SourceVersion: version, NLPLevel: 2},
}

func buildIntermediateParquets(l *lookup, maxCPUs int) {
	synonymFiles, err := filepath.Glob(fmt.Sprintf("%v/*.ndjson.lz4", synonyms))
	if err != nil {
		log.Fatal(err)
	}

	bar := uiprogress.AddBar(len(synonymFiles))
	bar.PrependElapsed()
	bar.PrependFunc(func(b *uiprogress.Bar) string {
		return "Building Intermediate Parquets:"
	})
	bar.AppendCompleted()

	var c *categoryCounter = &categoryCounter{categories: xsync.NewMapOf[string, uint32]()}
	var u *curieCounter = &curieCounter{}
	for shard := range shards {
		u.shard[shard].id = map[uint64]uint32{}
	}

	g := &errgroup.Group{}
	g.SetLimit(maxCPUs)
	for _, file := range synonymFiles {
		g.Go(func() error {
			f, err := os.Open(file)
			if err != nil {
				log.Fatal(err)
			}
			defer f.Close()

			rd := lz4.NewReader(f)
			dc := sonic.ConfigDefault.NewDecoder(rd)

			var synonyms map[uint][]synonymsSchema = map[uint][]synonymsSchema{}
			var curies map[uint][]curiesSchema = map[uint][]curiesSchema{}

			for {
				sj := synonymsJSON{}
				err = dc.Decode(&sj)
				if err == io.EOF {
					break
				} else if err != nil {
					log.Fatal(err)
				}

				curie := sj.Curie
				curie = clean(curie)
				if !tokenQC(curie) {
					continue
				}

				curieShard, h := getShard(curie)
				curID := u.GetCount(curieShard, h)

				preferred := sj.PreferredName
				preferred = clean(preferred)

				category := sj.Categories[0]

				categoryShard, _ := getShard(category)
				catID := c.GetCount(categoryShard, category)

				taxID := 0
				if len(sj.Taxon) > 0 {
					taxon := fmt.Sprintf("%v", sj.Taxon[0])
					taxon = strings.TrimPrefix(taxon, "NCBITaxon:")

					taxID, err = strconv.Atoi(taxon)
					if err != nil {
						log.Fatal(err)
					}
				}

				taxon := uint32(taxID)

				aliases := sj.Synonyms
				aliases = qcMultipleTokens(aliases, 1)

				if equiv, ok := l.Get(curieShard, h); ok {
					aliases = append(aliases, equiv...)
				}

				slices.Sort(aliases)
				aliases = slices.Compact(aliases)

				for _, alias := range aliases {
					shard, _ := getShard(alias)
					synonyms[shard] = append(synonyms[shard], synonymsSchema{CurieID: curID, SourceID: 1, Synonym: alias})
					curies[shard] = append(curies[shard], curiesSchema{CurieID: curID, Curie: curie, PreferredName: preferred, CategoryID: catID, Taxon: taxon})
				}

				aliases = qcMultipleTokens(aliases, 2)
				for _, alias := range aliases {
					shard, _ := getShard(alias)
					synonyms[shard] = append(synonyms[shard], synonymsSchema{CurieID: curID, SourceID: 2, Synonym: alias})
					curies[shard] = append(curies[shard], curiesSchema{CurieID: curID, Curie: curie, PreferredName: preferred, CategoryID: catID, Taxon: taxon})
				}
			}

			h := xxhash.Sum64String(file)
			hex := fmt.Sprintf("%016x", h)

			for shard := range shards {
				s := fmt.Sprintf("%v", shard)
				shardPath := fmt.Sprintf("%v/%v", parquets, s)

				synonymsFile := fmt.Sprintf("%v/%v.synonyms.parquet", shardPath, hex)
				err := parquet.WriteFile(synonymsFile, synonyms[shard])
				if err != nil {
					log.Fatal(err)
				}

				curiesFile := fmt.Sprintf("%v/%v.curies.parquet", shardPath, hex)
				err = parquet.WriteFile(curiesFile, curies[shard])
				if err != nil {
					log.Fatal(err)
				}
			}

			bar.Incr()
			return nil
		})
	}

	g.Wait()

	allCategories := c.BuildSchema()
	for shard := range shards {
		s := fmt.Sprintf("%v", shard)
		shardPath := fmt.Sprintf("%v/%v", parquets, s)

		h := xxhash.Sum64String(shardPath)
		hex := fmt.Sprintf("%016x", h)

		sourcesFile := fmt.Sprintf("%v/%v.sources.parquet", shardPath, hex)
		err := parquet.WriteFile(sourcesFile, hardcodedSources)
		if err != nil {
			log.Fatal(err)
		}

		categoriesFile := fmt.Sprintf("%v/%v.categories.parquet", shardPath, hex)
		err = parquet.WriteFile(categoriesFile, allCategories)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func clean(token string) string {
	cleaned := strings.TrimSpace(token)
	if token != cleaned {
		return clean(cleaned)
	} else if len(cleaned) <= 2 {
		return cleaned
	} else if cleaned[:2] == `""` || cleaned[:2] == `''` {
		return clean(cleaned[2:])
	}

	lastItem := len(cleaned) - 1
	if cleaned[:1] == `'` && cleaned[lastItem:] == `'` {
		return clean(cleaned[1:lastItem])
	} else if cleaned[:1] == `"` && cleaned[lastItem:] == `"` {
		return clean(cleaned[1:lastItem])
	}

	return cleaned
}

var bannedTokens []string = []string{
	"inchikey",
	"uncharacterized",
	"hypothetical",
}

func tokenQC(token string) bool {
	if token == "" || strings.ContainsAny(token, "\t\n\r") {
		return false
	}

	return !slices.Contains(bannedTokens, token)
}

var levelTwoRegex *regexp.Regexp = regexp.MustCompile(`\W+`)

func applyNLPLevel(token string, level int) string {
	switch level {
	case 1:
		return strings.ToLower(token)
	case 2:
		return levelTwoRegex.ReplaceAllString(token, "")
	default:
		return token
	}
}

func qcMultipleTokens(tokens []string, level int) []string {
	var passed []string = []string{}

	for _, token := range tokens {
		token = applyNLPLevel(token, level)
		token = clean(token)

		if tokenQC(token) {
			passed = append(passed, token)
		}
	}

	return passed
}

const shards uint = 16

func getShard(s string) (uint, uint64) {
	h := xxhash.Sum64String(s)
	return uint(h) % shards, h
}

type lookup struct {
	shard [shards]struct {
		mu    sync.Mutex
		class map[uint64]string
		// structure this so that total bytes is 64
		pad [48]byte
	}
}

func (l *lookup) Set(curie string, aliases []string) {
	shard, h := getShard(curie)

	s := &l.shard[shard]
	s.mu.Lock()
	defer s.mu.Unlock()

	s.class[h] = strings.Join(aliases, "\t")
}

func (l *lookup) Get(shard uint, h uint64) ([]string, bool) {
	s := &l.shard[shard]
	joined, ok := s.class[h]
	if !ok {
		return nil, false
	}

	aliases := strings.Split(joined, "\t")
	return aliases, ok
}

type classJSON struct {
	EquivalentIdentifiers []string `json:"equivalent_identifiers"`
}

func buildInMemoryLookup(maxCPUs int) *lookup {
	classFiles, err := filepath.Glob(fmt.Sprintf("%v/*.ndjson.lz4", classes))
	if err != nil {
		log.Fatal(err)
	}

	bar := uiprogress.AddBar(len(classFiles))
	bar.PrependElapsed()
	bar.PrependFunc(func(b *uiprogress.Bar) string {
		return "Building In Memory Lookup:"
	})
	bar.AppendCompleted()

	var l *lookup = &lookup{}
	for shard := range shards {
		l.shard[shard].class = map[uint64]string{}
	}

	g := &errgroup.Group{}
	g.SetLimit(maxCPUs)
	for _, file := range classFiles {
		g.Go(func() error {
			f, err := os.Open(file)
			if err != nil {
				log.Fatal(err)
			}
			defer f.Close()

			rd := lz4.NewReader(f)
			dc := sonic.ConfigDefault.NewDecoder(rd)

			for {
				cj := classJSON{}
				err = dc.Decode(&cj)
				if err == io.EOF {
					break
				} else if err != nil {
					log.Fatal(err)
				}

				equiv := cj.EquivalentIdentifiers
				if len(equiv) == 0 {
					continue
				}

				curie := equiv[0]
				aliases := equiv[1:]

				curie = clean(curie)
				if !tokenQC(curie) {
					continue
				}

				aliases = qcMultipleTokens(aliases, 1)
				l.Set(curie, aliases)
			}

			bar.Incr()
			return nil
		})
	}

	g.Wait()
	return l
}

func swapExt(path string, newExt string) string {
	protoExt := filepath.Ext(path)
	protostem := strings.TrimSuffix(path, protoExt)

	ext := filepath.Ext(protostem)
	stem := strings.TrimSuffix(protostem, ext)
	return stem + newExt
}

type chunkWriter struct {
	file *os.File
	lz4  *lz4.Writer
}

func (cw *chunkWriter) Write(data []byte) (int, error) {
	return cw.lz4.Write(data)
}

func (cw *chunkWriter) Close() {
	if err := cw.lz4.Close(); err != nil {
		cw.file.Close()
		log.Fatal(err)
	}

	if err := cw.file.Close(); err != nil {
		log.Fatal(err)
	}
}

func nextOpenChunk(filename string, chunk uint, dest string) (*chunkWriter, error) {
	chunkExt := fmt.Sprintf("%d.ndjson.lz4", chunk)
	chunkBasename := swapExt(filename, chunkExt)

	chunkPath := fmt.Sprintf("%v/%v", dest, chunkBasename)

	f, err := os.Create(chunkPath)
	if err != nil {
		return nil, err
	}

	var cw *chunkWriter = &chunkWriter{file: f, lz4: lz4.NewWriter(f)}
	return cw, nil
}

const maxRecords uint = 50000

func downloadAndSplit(filename string, url string, dest string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		return err
	}
	defer gz.Close()

	var count uint = 1
	var chunk uint = 1
	scanner := bufio.NewScanner(gz)
	// increased the buffer to 1MB bc 64KB was too small and errored
	scanner.Buffer(make([]byte, 0, 1024*1024), 1*1024*1024)

	out, err := nextOpenChunk(filename, chunk, dest)
	if err != nil {
		return err
	}
	chunk++

	for scanner.Scan() {
		if count%maxRecords == 0 {
			out.Close()

			out, err = nextOpenChunk(filename, chunk, dest)
			if err != nil {
				return err
			}
			chunk++
		}

		if _, err := fmt.Fprintln(out, scanner.Text()); err != nil {
			return err
		}
		count++
	}

	return scanner.Err()
}

const renci string = "https://stars.renci.org/var/babel_outputs"

func getBABELFiles(version string, endpoints []string, dataRegex *regexp.Regexp) [][2]string {
	var babelFiles [][2]string = [][2]string{}

	for _, endpoint := range endpoints {
		downloads := fmt.Sprintf("%v/%v/%v", renci, version, endpoint)

		resp, err := http.Get(downloads)
		if err != nil {
			log.Fatal(err)
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Fatal(err)
		}

		matches := dataRegex.FindAllStringSubmatch(string(body), -1)
		for _, match := range matches {
			// match[1] is just the filename instead of the whole html
			filename := match[1]
			url := fmt.Sprintf("%v/%v", downloads, filename)

			filename = filepath.Base(filename)
			filename = strings.ToLower(filename)
			babelFiles = append(babelFiles, [2]string{filename, url})
		}
	}

	return babelFiles
}

var classRegex *regexp.Regexp = regexp.MustCompile(`<a href="([^"]*_nodes[^"]*\.gz)"`)
var synonymRegex *regexp.Regexp = regexp.MustCompile(`<a href="([^"]+\.gz)"`)

var classEndpoints []string = []string{
	"kgx/",
}
var synonymEndpoints []string = []string{
	"synonyms/",
	"synonyms/chemicals/",
	"synonyms/geneprotein/",
}

func downloadBABEL(version string, endpoints []string, dest string, dataRegex *regexp.Regexp) {
	babelFiles := getBABELFiles(version, endpoints, dataRegex)

	bar := uiprogress.AddBar(len(babelFiles))
	bar.PrependElapsed()
	bar.PrependFunc(func(b *uiprogress.Bar) string {
		return fmt.Sprintf("Downloading BABEL Files To '%v' : \n", dest)
	})
	bar.AppendCompleted()

	for _, fileInfo := range babelFiles {
		filename := fileInfo[0]
		url := fileInfo[1]

		var err error
		var maxAttempts int = 5

		for range maxAttempts {
			err = downloadAndSplit(filename, url, dest)
			if err == nil {
				break
			}

			fileDownloadPattern := swapExt(filename, "*.ndjson.lz4")
			partiallyDownloadedFiles, _ := filepath.Glob(fmt.Sprintf("%v/%v", dest, fileDownloadPattern))
			for _, file := range partiallyDownloadedFiles {
				os.RemoveAll(file)
			}

			time.Sleep(5 * time.Second)
		}

		if err != nil {
			log.Fatal(err)
		}

		bar.Incr()
	}
}

const datassert string = "./datassert"
const parquets string = "./datassert/parquets"
const data string = "./datassert/data"
const downloads string = "./datassert/downloads"
const classes string = "./datassert/downloads/classes"
const synonyms string = "./datassert/downloads/synonyms"

func initializer() {
	perm := os.ModePerm

	var directories [6]string = [6]string{
		datassert,
		parquets,
		data,
		downloads,
		classes,
		synonyms,
	}

	for _, dir := range directories {
		os.MkdirAll(dir, perm)
	}

	for shard := range shards {
		s := fmt.Sprintf("%v", shard)
		os.MkdirAll(filepath.Join(parquets, s), perm)
	}
}

const version string = "2025sep1"

func build(cmd *cobra.Command, args []string) {
	// enable progress bar
	uiprogress.Start()
	defer uiprogress.Stop()

	// build required directories
	initializer()

	if !skipDownloads {
		// download classes and endpoints from BABEL
		downloadBABEL(version, classEndpoints, classes, classRegex)
		downloadBABEL(version, synonymEndpoints, synonyms, synonymRegex)
	}

	// get max cpus to use for parallelism
	maxCPUs := runtime.NumCPU() * 9 / 10

	if !useExistingParquets {
		// build in memory lookup
		l := buildInMemoryLookup(maxCPUs)

		// build intermediate parquets
		buildIntermediateParquets(l, maxCPUs)
	}

	// generate duckdb databases
	generateDuckDBs()
}

var skipDownloads bool
var useExistingParquets bool

var buildCmd = &cobra.Command{
	Use:   "build",
	Short: "Build an Optimized Sharded DuckDB Database for Biomedical NER from NCATS Translator BABEL",
	Run:   build,
}

func init() {
	rootCmd.AddCommand(buildCmd)
	buildCmd.Flags().BoolVarP(&skipDownloads, "skip-downloads", "s", false, "Skip the BABEL Download")
	buildCmd.Flags().BoolVarP(&useExistingParquets, "use-existing-parquets", "p", false, "Use Existing Parquets To Build Another DuckDB Database")

}
