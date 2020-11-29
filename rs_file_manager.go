package rsbackup

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"sort"

	"github.com/sirmackk/rsutils"

	log "github.com/sirupsen/logrus"
)

type RSFileManager struct {
	Config *Config
}

func (r *RSFileManager) ListData() ([]string, error) {
	dir, err := os.Open(r.Config.BackupRoot)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	n := 0
	for _, name := range names {
		matched, err := regexp.MatchString(`.*parity\.\d+$`, name)
		if err != nil {
			log.Errorf("Error while listing file '%s', skipping (error: %s)", name, err)
			continue
		}
		if !matched {
			names[n] = name
			n++
		}
	}
	names = names[:n]

	sort.Slice(names, func(i, j int) bool { return names[i] < names[j] })
	return names, nil
}

// ReadMetadata applies the naming scheme of "file" + ".md" to find
// and read the metadata of the file at "fpath"
func (r *RSFileManager) ReadMetadata(fpath string) (*rsutils.Metadata, error) {
	mdPath := fpath + ".md"
	mdFile, err := os.Open(mdPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Errorf("Metadata file '%s' does not exist!", mdPath)
			return nil, fmt.Errorf("Metadata not found")
		}
		log.Errorf("Cannot open metadata file '%s': %s", mdPath, err)
		return nil, err
	}
	var md rsutils.Metadata
	err = json.NewDecoder(mdFile).Decode(&md)
	if err != nil {
		log.Errorf("Unable to decode metadata '%s': %s", mdPath, err)
		return nil, err
	}
	return &md, nil
}

func (r *RSFileManager) WriteMetadata(fname string, md *rsutils.Metadata) error {
	fpath := path.Join(r.Config.BackupRoot, fname)
	mdPath := fpath + ".md"
	mdFile, err := os.OpenFile(mdPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0655)
	if err != nil {
		log.Errorf("Cannot create metadata file %s: %s", mdPath, err)
		return err
	}
	err = json.NewEncoder(mdFile).Encode(md)
	if err != nil {
		log.Errorf("Unable to encode metadata to %s: %s", mdPath, err)
		return err
	}
	return nil
}

func (r *RSFileManager) SaveFile(src io.Reader, fname string) (string, error) {
	dstPath := path.Join(r.Config.BackupRoot, fname)
	outputFile, err := os.OpenFile(dstPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0655)
	if err != nil {
		return "", err
	}
	defer outputFile.Close()
	_, err = io.Copy(outputFile, src)
	if err != nil {
		return "", err
	}
	return dstPath, nil
}

func (rs *RSBackupAPI) GenerateParityFiles(dataFilePath string) (*rsutils.Metadata, error) {
	dataFile, err := os.Open(dataFilePath)
	if err != nil {
		return nil, err
	}
	defer dataFile.Close()
	dataFileStat, err := dataFile.Stat()
	if err != nil {
		return nil, err
	}
	dataFileSize := dataFileStat.Size()
	dataShards := rs.Config.DataShards
	parityShards := rs.Config.ParityShards

	dataChunks := rsutils.SplitIntoPaddedChunks(dataFile, dataFileSize, dataShards)
	dataSources := make([]io.Reader, len(dataChunks))
	for i := range dataChunks {
		dataSources[i] = dataChunks[i]
	}
	parityWriters := make([]io.Writer, rs.Config.ParityShards)
	for i := range parityWriters {
		parityPath := fmt.Sprintf("%s.parity.%d", dataFilePath, i+1)
		pwriter, err := os.OpenFile(parityPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0655)
		if err != nil {
			return nil, err
		}
		defer pwriter.Close()
		parityWriters[i] = pwriter
	}
	shardCreator := rsutils.NewShardCreator(dataSources, dataFileSize, dataShards, parityShards)
	return shardCreator.Encode(parityWriters)
}

func (r *RSFileManager) CheckData(fname string) (bool, string, []string, error) {
	fpath := path.Join(r.Config.BackupRoot, fname)
	dataFile, err := os.Open(fpath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Errorf("Requested file '%s' does not exist", fpath)
			return false, "", []string{}, fmt.Errorf("File not found")
		}
		log.Errorf("Cannot open file '%s': %s", fpath, err)
		return false, "", []string{}, err
	}
	defer dataFile.Close()
	md, err := r.ReadMetadata(fpath)
	if err != nil {
		return false, "", []string{}, err
	}

	fileChunks := rsutils.SplitIntoPaddedChunks(dataFile, md.Size, md.DataShards)
	shards := make([]io.ReadWriteSeeker, len(fileChunks)+md.ParityShards)
	for i := range fileChunks {
		shards[i] = fileChunks[i]
	}
	for i := 0; i < md.ParityShards; i++ {
		parityPath := fmt.Sprintf("%s.parity.%d", fpath, i+1)
		parityChunk, err := os.Open(parityPath)
		if err != nil {
			return false, "", []string{}, err
		}
		defer parityChunk.Close()
		shards[md.DataShards+i] = parityChunk
	}

	shardMan := rsutils.NewShardManager(shards, md)
	err = shardMan.CheckHealth()
	var health = true
	if err != nil {
		log.Infof("Found corrupted shards for '%s': %s", fpath, err)
		health = false
	}

	stat, err := dataFile.Stat()
	if err != nil {
		log.Errorf("Cannot stat file '%s': %s", fpath, err)
		return false, "", []string{}, err
	}
	lmod := stat.ModTime().Format("2006-01-02 15:04:05")
	return health, lmod, md.Hashes, nil
}
