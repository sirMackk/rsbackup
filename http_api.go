package rsbackup

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

type Config struct {
	backupRoot   string
	dataShards   int
	parityShards int
	address      string
	httpCertPath string
	httpKeyPath  string
}

func getClientIP(r *http.Request) string {
	if addr := r.Header.Get("x-forwarded-for"); addr != "" {
		return addr
	} else if addr := r.RemoteAddr; addr != "" {
		return addr
	} else {
		return "Unknown"
	}
}

// getURLParam returns the parameter in a URL.
// It is specifically limited to returning only the 3rd level part, ie.
// /some/thing will return "thing."
func getURLParam(urlPath string) (string, error) {
	urlParams := strings.Split(urlPath, "/")
	if len(urlParams) != 3 || urlParams[2] == "" {
		return "", fmt.Errorf("Cannot extract url param from '%s'", urlPath)
	}
	return urlParams[2], nil
}

type RSBackupAPI struct {
	server    *http.Server
	config    *Config
	rsFileMan *RSFileManager
}

func (rs *RSBackupAPI) Errorf(r *http.Request, formatString string, args ...interface{}) {
	fmtString := fmt.Sprintf("[%s] %s", getClientIP(r), formatString)
	log.Errorf(fmtString, args...)
}

func (r *RSBackupAPI) Start() chan struct{} {
	r.server = &http.Server{
		Addr: r.config.address,
	}

	running := make(chan struct{})

	go func() {
		r.registerRoutes()
		err := r.server.ListenAndServeTLS(r.config.httpCertPath, r.config.httpKeyPath)
		if err != nil {
			log.Errorf("TLS Server couldn't start: %s", err)
			close(running)
		}
	}()
	return running
}

func (r *RSBackupAPI) Stop() error {
	log.Infof("Shutting down server...")
	if r.server != nil {
		err := r.server.Shutdown(context.Background())
		if err != nil {
			return fmt.Errorf("Error while shutting down server: %s", err)
		}
		r.server = nil
		log.Info("Server shutdown successfully")
	}
	return nil
}

func (r *RSBackupAPI) registerRoutes() {
	http.HandleFunc("/list_data", r.listDataHandler)
	http.HandleFunc("/check_data", r.checkDataHandler)
	http.HandleFunc("/submit_data", r.submitDataHandler)
	http.HandleFunc("/retrieve_data", r.retrieveDataHandler)
}

type listDataRsp struct {
	Files []string `json:"files"`
}

func (rs *RSBackupAPI) listDataHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		rs.Errorf(r, "Bad request method %s", r.Method)
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
	names, err := rs.rsFileMan.ListData()
	if err != nil {
		rs.Errorf(r, "Error while listing files from %s: %s", rs.config.backupRoot, err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(&listDataRsp{Files: names})
	if err != nil {
		rs.Errorf(r, "Error while marshalling json: %s", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
}

type checkDataRsp struct {
	Name   string   `json:"name"`
	Lmod   string   `json:"lmod"`
	Health bool     `json:"health"`
	Hashes []string `json:"hashes"`
}

func (rs *RSBackupAPI) checkDataHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		rs.Errorf(r, "Bad request method %s", r.Method)
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
	fname, err := getURLParam(r.URL.Path)
	if err != nil {
		rs.Errorf(r, "Can't check data: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	health, lmod, hashes, err := rs.rsFileMan.CheckData(fname)
	if err != nil {
		if err.Error() == "File not found" {
			rs.Errorf(r, "File %s not found", fname)
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
			return
		}
		rs.Errorf(r, "Could not process request: %s", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	rsp := &checkDataRsp{
		Name:   fname,
		Lmod:   lmod,
		Health: health,
		Hashes: hashes,
	}
	w.Header().Set("Content-Type", "application/json")
	err = json.NewEncoder(w).Encode(rsp)
	if err != nil {
		rs.Errorf(r, "Unable to marshal json response: %s", err)
	}
}

type submitDataRsp struct {
	Size         int64    `json:"size"`
	Hashes       []string `json:"hashes"`
	DataShards   int      `json:"data_shards"`
	ParityShards int      `json:"parity_shards"`
}

func (rs *RSBackupAPI) submitDataHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		rs.Errorf(r, "Bad method %s", r.Method)
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
	err := r.ParseMultipartForm(256 << 20)
	if err != nil {
		rs.Errorf(r, "Error while reading multipart form: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	inputData, header, err := r.FormFile("file")
	if err != nil {
		rs.Errorf(r, "Bad form field: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	defer inputData.Close()
	log.Errorf("Creating file %s", header.Filename)
	dataFilePath, err := rs.rsFileMan.SaveFile(inputData, header.Filename)
	if err != nil {
		// TODO: bubble up 'file exists' error to client somehow
		rs.Errorf(r, "Unable to save file %s: %s", header.Filename, err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	md, err := rs.GenerateParityFiles(dataFilePath)
	if err != nil {
		// TODO: bubble up 'file exists' error to client somehow
		rs.Errorf(r, "Unable to generate parity files for %s: %s", header.Filename, err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	rsp := &submitDataRsp{
		Size:         md.Size,
		Hashes:       md.Hashes,
		DataShards:   md.DataShards,
		ParityShards: md.ParityShards,
	}

	w.Header().Set("content-type", "application/json")
	err = json.NewEncoder(w).Encode(rsp)
	if err != nil {
		rs.Errorf(r, "Error while encoding json: %s", err)
	}
}

func (rs *RSBackupAPI) retrieveDataHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		rs.Errorf(r, "Bad method %s", r.Method)
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
	fname, err := getURLParam(r.URL.Path)
	if err != nil {
		rs.Errorf(r, "Can't retrieve file: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	fpath := path.Join(rs.config.backupRoot, fname)
	file, err := os.Open(fpath)
	if err != nil {
		if os.IsNotExist(err) {
			rs.Errorf(r, "Retrieval failed, %s does not exist", fpath)
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
			return
		}
		rs.Errorf(r, "Retrieval of %s failed: %s", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	defer file.Close()
	http.ServeContent(w, r, fname, time.Time{}, file)
}
