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
	"fmt"
	"testing"

	"github.com/compose/transporter/client"
)

func TestReader(t *testing.T) {
	cl, db, err := Backchannel(TestUser, TestPass, TestURI)
	if err != nil {
		t.Fatal("Failed to obtain backchannel")
	}

	CloudantAdaptor.Database = db.Name
	c, err := CloudantAdaptor.Client()
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

	rd, err := CloudantAdaptor.Reader()
	if err != nil {
		t.Errorf("Failed to start Cloudant Reader, %s", err)
	}

	uploader := db.Bulk(testDocCount, 1024*1024, 0)

	// Insert 10 docs
	docs := make([]interface{}, testDocCount)
	for i := 0; i < testDocCount; i++ {
		docs[i] = struct {
			Foo string `json:"foo"`
			Bar string `json:"bar"`
		}{
			UuidIsh(),
			UuidIsh(),
		}
	}

	MakeDocs(uploader, docs)
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
