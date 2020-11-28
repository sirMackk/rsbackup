package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"

	log "github.com/sirupsen/logrus"

	"github.com/sirmackk/rsbackup"
)

func setupLogging(debug, ts bool) {
	if debug {
		log.SetLevel(log.DebugLevel)
		log.Debug("Debug logging enabled")
	} else {
		log.SetLevel(log.InfoLevel)
	}
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: ts,
	})
}

func main() {
	var ip = flag.String("ip", "127.0.0.1", "Iface address to bind to")
	var port = flag.Int("port", 44987, "Port to bind to")
	var dataShards = flag.Int("data-shards", 10, "Number of data shards")
	var parityShards = flag.Int("parity-shards", 3, "Number of parity shards")
	var backupRoot = flag.String("backup-root", ".", "Directory to store data & parity")
	var httpCertPath = flag.String("cert-path", "", "Path to TLS certificate for HTTP server")
	var httpKeyPath = flag.String("key-path", "", "Path to TLS certificate key")
	var debug = flag.Bool("debug", false, "Enable debug logging")
	var tsLogging = flag.Bool("timestamp-logging", false, "Enable log timestamps")
	flag.Parse()

	if *httpCertPath == "" || *httpKeyPath == "" {
		log.Error("both -cert-path and -key-path arguments are required!")
		os.Exit(1)
	}

	setupLogging(*debug, *tsLogging)

	config := &rsbackup.Config{
		BackupRoot:   *backupRoot,
		DataShards:   *dataShards,
		ParityShards: *parityShards,
		HttpCertPath: *httpCertPath,
		HttpKeyPath:  *httpKeyPath,
		Address:      fmt.Sprintf("%s:%d", *ip, *port),
	}
	rsMan := &rsbackup.RSFileManager{
		Config: config,
	}

	apiServer := &rsbackup.RSBackupAPI{
		Config:    config,
		RsFileMan: rsMan,
	}

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	go func() {
		sig := <-terminate
		log.Infof("Received signal %s, terminating...", sig)
		err := apiServer.Stop()
		if err != nil {
			log.Errorf("Error while shutting down server: %s", err)
			os.Exit(1)
		}
		os.Exit(0)
	}()
	log.Debugf("Starting server using config: %#v", config)
	<-apiServer.Start()
}
