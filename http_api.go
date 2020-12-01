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
	BackupRoot   string
	DataShards   int
	ParityShards int
	Address      string
	HttpCertPath string
	HttpKeyPath  string
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
	Config    *Config
	RsFileMan *RSFileManager
	server    *http.Server
}

func (rs *RSBackupAPI) Errorf(r *http.Request, formatString string, args ...interface{}) {
	fmtString := fmt.Sprintf("[%s] %s", getClientIP(r), formatString)
	log.Errorf(fmtString, args...)
}

func (r *RSBackupAPI) Start() chan struct{} {
	r.server = &http.Server{
		Addr: r.Config.Address,
	}
	running := make(chan struct{})

	go func() {
		r.registerRoutes()
		err := r.server.ListenAndServeTLS(r.Config.HttpCertPath, r.Config.HttpKeyPath)
		if err != nil {
			log.Errorf("TLS Server couldn't start: %s", err)
			close(running)
		}
	}()
	log.Infof("Started http api server on %s", r.Config.Address)
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
	log.Debug("Registering routes")
	http.HandleFunc("/list_data", r.listDataHandler)
	http.HandleFunc("/check_data/", r.checkDataHandler)
	http.HandleFunc("/submit_data", r.submitDataHandler)
	http.HandleFunc("/retrieve_data/", r.retrieveDataHandler)
	http.HandleFunc("/repair_data/", r.repairDataHandler)
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
	log.Debugf("Listing files in %s", rs.Config.BackupRoot)
	names, err := rs.RsFileMan.ListData()
	if err != nil {
		rs.Errorf(r, "Error while listing files from %s: %s", rs.Config.BackupRoot, err)
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
	log.Debugf("Checking health of %s", fname)
	health, lmod, hashes, err := rs.RsFileMan.CheckData(fname)
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
	// TODO: data/parity shards should be set through request, not config
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
	inputData, _, err := r.FormFile("file")
	if err != nil {
		rs.Errorf(r, "Bad form field: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	defer inputData.Close()
	desiredFileName := r.FormValue("filename")
	if desiredFileName == "" {
		rs.Errorf(r, "Missing 'filename' parameter'", "")
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	if strings.ContainsAny(desiredFileName, "/") {
		rs.Errorf(r, "Request contains forbidden character '/' in filename '%s',", desiredFileName)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	log.Debugf("Submitted file %s", desiredFileName)
	dataFilePath, err := rs.RsFileMan.SaveFile(inputData, desiredFileName)
	if err != nil {
		// TODO: bubble up 'file exists' error to client somehow
		rs.Errorf(r, "Unable to save file %s: %s", desiredFileName, err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	md, err := rs.GenerateParityFiles(dataFilePath)
	if err != nil {
		// TODO: bubble up 'file exists' error to client somehow
		rs.Errorf(r, "Unable to generate parity files for %s: %s", desiredFileName, err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	err = rs.RsFileMan.WriteMetadata(desiredFileName, md)
	if err != nil {
		rs.Errorf(r, "%s", err)
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
	fpath := path.Join(rs.Config.BackupRoot, fname)
	log.Debugf("Retrieving file %s", fpath)
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

type repairDataRsp struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

func (rs *RSBackupAPI) repairDataHandler(w http.ResponseWriter, r *http.Request) {
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
	rsp := &repairDataRsp{
		Name:   fname,
		Status: "GOOD",
	}
	w.Header().Set("Content-Type", "application/json")
	log.Debugf("Repairing file %s", fname)
	err = rs.RsFileMan.RepairData(fname)
	if err != nil {
		if os.IsNotExist(err) {
			rs.Errorf(r, "File %s not found", fname)
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
			return
		}
		rs.Errorf(r, "Could not process request: %s", err)
		// TODO: find better way to bubble up specific errors
		if strings.HasPrefix(err.Error(), "Cannot repair data") || strings.HasPrefix(err.Error(), "Error reconstructing data") {
			rsp.Status = err.Error()
			err = json.NewEncoder(w).Encode(rsp)
			if err != nil {
				rs.Errorf(r, "Cannot marshal json rsp: %s", err)
			}
			return
		}
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	err = json.NewEncoder(w).Encode(rsp)
	if err != nil {
		rs.Errorf(r, "Cannot mashal json rsp: %s", err)
	}
}
