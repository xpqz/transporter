package cloudant

// Assume local couchdb is running on localhost:5984
//
// To run: go test -v ./adaptor/cloudant/...
//
// To run tests against a local CouchDB:
//
// % docker run -d -p 5984:5984 --rm --name couchdb couchdb:1.6
// % export HOST="http://127.0.0.1:5984"
// % curl -XPUT $HOST/_config/admins/admin -d '"xyzzy"'
// % curl -XPUT $HOST/testdb
// % curl -XPUT $HOST/testdb -u admin

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
	cdt "github.ibm.com/cloudant/go-cloudant"
)

const (
	testUser = "admin"
	testPass = "xyzzy"
	testURI  = "http://127.0.0.1:5984"
)

var (
	cloudantAdaptor = &cloudant{
		BaseConfig:  adaptor.BaseConfig{URI: DefaultURI},
		username:    testUser,
		password:    testPass,
		Tail:        true,
		batchsize:   500,
		timeout:     0,
		seqInterval: 10,
	}
)

func TestDescription(t *testing.T) {
	if cloudantAdaptor.Description() != description {
		t.Errorf("wrong description returned, expected %s, got %s", description, cloudantAdaptor.Description())
	}
}

func TestSampleConfig(t *testing.T) {
	if cloudantAdaptor.SampleConfig() != sampleConfig {
		t.Errorf("wrong config returned, expected %s, got %s", sampleConfig, cloudantAdaptor.SampleConfig())
	}
}

func TestWriter(t *testing.T) {
	cl, db, err := backchannel(testUser, testPass, testURI)
	cloudantAdaptor.Database = db.Name

	c, err := cloudantAdaptor.Client()
	if err != nil {
		t.Fatal("Failed to start Cloudant Client")
	}

	s, err := c.Connect()
	done := make(chan struct{})
	defer func() {
		cl.Delete(db.Name)
		cl.LogOut()
		cl.Stop()
		c.(*Client).Close()
		close(done)
	}()

	if err != nil {
		t.Fatalf("unable to obtain session to cloudant, %s", err)
	}

	var wg sync.WaitGroup
	wr, err := cloudantAdaptor.Writer(done, &wg)
	if err != nil {
		t.Errorf("Failed to start Cloudant Writer, %s", err)
	}

	defer wr.(*Writer).Close()

	confirms := make(chan struct{})
	var confirmed bool
	go func() {
		<-confirms
		confirmed = true
	}()

	for i := 0; i < 999; i++ {
		msg := message.From(ops.Insert, "bulk", map[string]interface{}{"foo": i, "i": i})
		if _, err := wr.Write(message.WithConfirms(confirms, msg))(s); err != nil {
			t.Errorf("unexpected Insert error, %s", err)
		}
	}
}

func TestReader(t *testing.T) {
	cl, db, err := backchannel(testUser, testPass, testURI)
	if err != nil {
		t.Fatal("Failed to obtain backchannel")
	}

	cloudantAdaptor.Tail = false // Single batch changes
	cloudantAdaptor.Database = db.Name
	c, err := cloudantAdaptor.Client()
	testDocCount := 10
	if err != nil {
		t.Fatal("Failed to start Cloudant Client")
	}

	s, err := c.Connect()
	done := make(chan struct{})
	defer func() {
		fmt.Printf("Deleting database %s", db.Name)
		cl.Delete(db.Name)
		cl.LogOut()
		cl.Stop()
		c.(*Client).Close()
		close(done)
	}()

	if err != nil {
		t.Fatalf("unable to obtain Cloudant session, %s", err)
	}

	rd, err := cloudantAdaptor.Reader()
	if err != nil {
		t.Errorf("Failed to start Cloudant Reader, %s", err)
	}

	uploader := db.Bulk(testDocCount, 0)

	// Insert 10 docs
	docs := make([]interface{}, testDocCount)
	for i := 0; i < testDocCount; i++ {
		docs[i] = struct {
			Foo string `json:"foo"`
			Bar string `json:"bar"`
		}{
			uuidIsh(),
			uuidIsh(),
		}
	}

	makeDocs(uploader, docs)
	batcher := rd.Read(map[string]client.MessageSet{}, func(ns string) bool {
		return true
	})

	changes, err := batcher(s, done)
	if err != nil {
		t.Fatalf("unexpected Read error, %s\n", err)
	}
	var numMsgs int
	for _ = range changes {
		numMsgs++
	}
	if numMsgs != testDocCount {
		t.Errorf("bad message count, expected %d, got %d\n", testDocCount, numMsgs)
	}
	rd.(*Reader).Close()
}

func TestTailer(t *testing.T) {
	cl, db, err := backchannel(testUser, testPass, testURI)
	if err != nil {
		t.Fatal("Failed to obtain backchannel")
	}
	testDocCount := 10
	cloudantAdaptor.Tail = true
	cloudantAdaptor.Database = db.Name

	c, err := cloudantAdaptor.Client()
	if err != nil {
		t.Fatal("Failed to start Cloudant Client")
	}

	s, err := c.Connect()
	done := make(chan struct{})
	defer func() {
		fmt.Printf("Deleting database %s", db.Name)
		cl.Delete(db.Name)
		cl.LogOut()
		cl.Stop()
		c.(*Client).Close()
		close(done)
	}()

	if err != nil {
		t.Fatalf("unable to obtain session to cloudant, %s", err)
	}

	tailer, err := cloudantAdaptor.Reader()
	if err != nil {
		t.Errorf("Failed to start Cloudant Tailer, %s", err)
	}

	followChanges := tailer.Read(map[string]client.MessageSet{}, func(table string) bool {
		return true
	})

	defer tailer.(*Tailer).Close()

	msgChan, err := followChanges(s, done)
	if err != nil {
		t.Fatalf("unexpected Tail error, %s\n", err)
	}

	uploader := db.Bulk(testDocCount, 0)

	// Insert testDocCount docs
	docs := []interface{}{}
	for i := 0; i < testDocCount; i++ {
		docs = append(docs, struct {
			Foo string `json:"foo"`
			Bar string `json:"bar"`
		}{
			uuidIsh(),
			uuidIsh(),
		})
	}

	vitals := makeDocs(uploader, docs)
	checkCount("inserted", testDocCount, msgChan, t)

	docs = []interface{}{}

	// Update testDocCount docs
	for i := 0; i < testDocCount; i++ {
		docs = append(docs, struct {
			ID  string `json:"_id"`
			Rev string `json:"_rev"`
			Foo string `json:"foo"`
			Bar string `json:"bar"`
		}{
			vitals[i].ID,
			vitals[i].Rev,
			uuidIsh(),
			uuidIsh(),
		})
	}

	vitals = makeDocs(uploader, docs)
	checkCount("updated data", testDocCount, msgChan, t)

	// Delete 10 docs
	docs = []interface{}{}
	for i := 0; i < testDocCount; i++ {
		docs = append(docs, struct {
			ID      string `json:"_id"`
			Rev     string `json:"_rev"`
			Deleted bool   `json:"_deleted"`
		}{
			vitals[i].ID,
			vitals[i].Rev,
			true,
		})
	}

	vitals = makeDocs(uploader, docs)
	checkCount("deleted data", testDocCount, msgChan, t)
}

// This is rather crude -- expect problems if testing against a non-local
// cluster.
func checkCount(desc string, expected int, msgChan <-chan client.MessageSet, t *testing.T) {
	var numMsgs int
	var wg sync.WaitGroup
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		for {
			select {
			case <-msgChan:
				numMsgs++
			case <-time.After(1 * time.Second):
				if numMsgs == expected {
					wg.Done()
					return
				}
			case <-time.After(5 * time.Second):
				wg.Done()
				return
			}
		}
	}(&wg)
	wg.Wait()
	if numMsgs != expected {
		t.Errorf("[%s] bad message count, expected %d, got %d\n", desc, expected, numMsgs)
	}
}

func makeDocs(uploader *cdt.Uploader, docs []interface{}) []cdt.BulkDocsResponse {
	result, err := uploader.BulkUploadSimple(docs)
	if err != nil {
		log.Printf("%s\n", err)
	}
	return result
}

func uuidIsh() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return ""
	}

	uuid := fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])

	return uuid
}

func dbName() string {
	return fmt.Sprintf("transporter-%x", sha256.Sum256([]byte(uuidIsh())))
}

func backchannel(username, password, baseURL string) (*cdt.CouchClient, *cdt.Database, error) {
	client, err := cdt.CreateClient(username, password, baseURL, 1)
	if err != nil {
		return nil, nil, err
	}
	database, err := client.GetOrCreate(dbName())
	if err != nil {
		return nil, nil, err
	}

	return client, database, nil
}
