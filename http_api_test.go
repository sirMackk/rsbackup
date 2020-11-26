package rsbackup

import (
	//"fmt"
	"bytes"
	"io"
	"io/ioutil"
	"mime/multipart"
	"os"
	"path"

	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func createTMPDir(t *testing.T, name string) string {
	tmpDir, err := ioutil.TempDir("", name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		os.RemoveAll(tmpDir)
	})
	return tmpDir
}

func fillDirWithEmptyFiles(t *testing.T, dir string, names ...string) []string {
	for _, name := range names {
		f, err := os.Create(path.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
	}
	return names
}

func TestListDataHandler(t *testing.T) {
	tmpDir := createTMPDir(t, "rsbackup")
	fillDirWithEmptyFiles(t, tmpDir, "file1", "file2", "file1.parity.1", "file1.parity.2")

	listDataTests := []struct {
		name           string
		method         string
		directory      string
		expectedStatus int
		expectedRsp    string
		expectedHeader string
	}{
		{"good request", "GET", tmpDir, 200, `{"files":["file1","file2"]}`, "application/json"},
		{"bad method", "POST", tmpDir, 405, "Method Not Allowed", "text/plain; charset=utf-8"},
		{"bad backupRoot dir", "GET", "/dir/doesnt/exist", 500, "Internal Server Error", "text/plain; charset=utf-8"},
	}

	for _, tt := range listDataTests {
		t.Run(tt.name, func(t *testing.T) {
			config := &Config{
				backupRoot: tt.directory,
			}
			api := &RSBackupAPI{
				config: config,
				rsFileMan: &RSFileManager{
					config: config,
				},
			}

			req, err := http.NewRequest(tt.method, "/list_data", nil)
			if err != nil {
				t.Fatal(err)
			}
			rr := httptest.NewRecorder()
			handler := http.HandlerFunc(api.listDataHandler)

			handler.ServeHTTP(rr, req)
			rsp := rr.Result()

			if rsp.StatusCode != tt.expectedStatus {
				t.Errorf("Got status code %d, expected %d", rsp.StatusCode, tt.expectedStatus)
			}

			body, err := ioutil.ReadAll(rsp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if rspBody := strings.TrimSuffix(string(body), "\n"); rspBody != tt.expectedRsp {
				t.Errorf("Got rsp body '%s', expected '%s'", rspBody, tt.expectedRsp)
			}
			if contentType := rsp.Header.Get("content-type"); contentType != tt.expectedHeader {
				t.Errorf("Got content-type header '%s'; expected '%s'", contentType, tt.expectedHeader)
			}
		})
	}
}

func TestCheckDataHandler(t *testing.T) {
	checkDataTests := []struct {
		name           string
		method         string
		url            string
		expectedStatus int
		expectedRsp    string
	}{
		{"bad method", "POST", "/check_data/tyger", 405, "Method Not Allowed"},
		{"bad url param", "GET", "/check_data/", 400, "Bad Request"},
		{"file not found", "GET", "/check_data/lion", 404, "Not Found"},
		{"file check success", "GET", "/check_data/tyger", 200, `{"name":"tyger","lmod":"2020-11-24 11:34:23","health":true,"hashes":["aa8b8979f1486fe03d54d1bdd4a32018386285a2ad0dc9a2820f0da3d6293e72","64163fa75b3eadb78f376dd7ab84e48595e9748dadbfb50e2126bef20481baa1","e32a8903342ab6dc68d46462df727f6812f6fbb728c4a1240b625331b811c147"]}`},
		{"file check failed", "GET", "/check_data/tyger_bad", 200, `{"name":"tyger_bad","lmod":"2020-11-24 14:07:39","health":false,"hashes":["aa8b8979f1486fe03d54d1bdd4a32018386285a2ad0dc9a2820f0da3d6293e72","64163fa75b3eadb78f376dd7ab84e48595e9748dadbfb50e2126bef20481baa1","e32a8903342ab6dc68d46462df727f6812f6fbb728c4a1240b625331b811c147"]}`},
	}

	for _, tt := range checkDataTests {
		t.Run(tt.name, func(t *testing.T) {
			config := &Config{
				backupRoot: "testdata/",
			}
			api := &RSBackupAPI{
				config: config,
				rsFileMan: &RSFileManager{
					config: config,
				},
			}

			req, err := http.NewRequest(tt.method, tt.url, nil)
			if err != nil {
				t.Fatal(err)
			}
			rr := httptest.NewRecorder()
			handler := http.HandlerFunc(api.checkDataHandler)

			handler.ServeHTTP(rr, req)
			rsp := rr.Result()

			if rsp.StatusCode != tt.expectedStatus {
				t.Errorf("Got status code %d, expected %d", rsp.StatusCode, tt.expectedStatus)
			}
			body, err := ioutil.ReadAll(rsp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if rspBody := strings.TrimSuffix(string(body), "\n"); rspBody != tt.expectedRsp {
				t.Errorf("Got rsp body '%s', expected '%s'", rspBody, tt.expectedRsp)
			}
		})
	}
}

func TestSubmitDataHandler(t *testing.T) {

	submitDataTests := []struct {
		name           string
		method         string
		fileToSubmit   string
		filesThatExist []string
		formFileField  string
		expectedStatus int
		expectedRsp    string
	}{
		{"bad method", "GET", "tyger", []string{}, "file", 405, "Method Not Allowed"},
		{"bad form field", "POST", "tyger", []string{}, "derp", 400, "Bad Request"},
		{"file exists", "POST", "tyger", []string{"tyger"}, "file", 500, "Internal Server Error"},
		{"parity file exists", "POST", "tyger", []string{"tyger.parity.1"}, "file", 500, "Internal Server Error"},
		{"successful upload", "POST", "tyger", []string{}, "file", 200, `{"size":808,"hashes":["e4ee8bee30da3556436d715cf24da735445eb7b735d96d9976cb4f64d7043a18","64163fa75b3eadb78f376dd7ab84e48595e9748dadbfb50e2126bef20481baa1","e32a8903342ab6dc68d46462df727f6812f6fbb728c4a1240b625331b811c147"],"data_shards":2,"parity_shards":1}`},
	}
	// successful upload
	for _, tt := range submitDataTests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := createTMPDir(t, "rsbackup")
			config := &Config{
				backupRoot:   tmpDir,
				dataShards:   2,
				parityShards: 1,
			}
			api := &RSBackupAPI{
				config: config,
				rsFileMan: &RSFileManager{
					config: config,
				},
			}

			fillDirWithEmptyFiles(t, tmpDir, tt.filesThatExist...)

			body := new(bytes.Buffer)
			multipartWriter := multipart.NewWriter(body)
			form, err := multipartWriter.CreateFormFile(tt.formFileField, tt.fileToSubmit)
			if err != nil {
				t.Fatal(err)
			}
			fileToSubmit, err := os.Open("testdata/" + tt.fileToSubmit)
			if err != nil {
				t.Fatal(err)
			}
			defer fileToSubmit.Close()
			_, err = io.Copy(form, fileToSubmit)
			if err != nil {
				t.Fatal(err)
			}
			multipartWriter.Close()
			req := httptest.NewRequest(tt.method, "/submit_data", body)
			req.Header.Add("content-type", multipartWriter.FormDataContentType())
			rr := httptest.NewRecorder()
			handler := http.HandlerFunc(api.submitDataHandler)

			handler.ServeHTTP(rr, req)
			rsp := rr.Result()

			if rsp.StatusCode != tt.expectedStatus {
				t.Errorf("Got status code %d, expected %d", rsp.StatusCode, tt.expectedStatus)
			}
			rspBody, err := ioutil.ReadAll(rsp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if rspBodyTrimmed := strings.TrimSuffix(string(rspBody), "\n"); rspBodyTrimmed != tt.expectedRsp {
				t.Errorf("Got rsp body '%s', expected '%s'", rspBodyTrimmed, tt.expectedRsp)
			}

		})
	}
}

func TestRetrieveDataHandler(t *testing.T) {
	testData, err := ioutil.ReadFile("testdata/tyger")
	if err != nil {
		t.Fatal(err)
	}
	expectedTestData := strings.TrimSuffix(string(testData), "\n")
	retrieveDataTests := []struct {
		name           string
		method         string
		url            string
		expectedStatus int
		expectedRsp    string
	}{
		{"bad method", "DELETE", "/retrieve_data/tyger", 405, "Method Not Allowed"},
		{"bad url", "GET", "/retrieve_data/tyger/tail", 400, "Bad Request"},
		{"file not found", "GET", "/retrieve_data/lion", 404, "Not Found"},
		{"success", "GET", "/retrieve_data/tyger", 200, expectedTestData},
	}

	for _, tt := range retrieveDataTests {
		t.Run(tt.name, func(t *testing.T) {
			config := &Config{
				backupRoot: "testdata/",
			}
			api := &RSBackupAPI{
				config: config,
				rsFileMan: &RSFileManager{
					config: config,
				},
			}

			req := httptest.NewRequest(tt.method, tt.url, nil)
			rr := httptest.NewRecorder()
			handler := http.HandlerFunc(api.retrieveDataHandler)
			handler.ServeHTTP(rr, req)
			rsp := rr.Result()

			if rsp.StatusCode != tt.expectedStatus {
				t.Errorf("Got status code %d, expected %d", rsp.StatusCode, tt.expectedStatus)
			}
			rspBody, err := ioutil.ReadAll(rsp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if rspBodyTrimmed := strings.TrimSuffix(string(rspBody), "\n"); rspBodyTrimmed != tt.expectedRsp {
				t.Errorf("Got rsp body '%s', expected '%s'", rspBodyTrimmed, tt.expectedRsp)
			}
		})
	}
}
