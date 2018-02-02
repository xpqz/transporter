package cloudant

import (
	"testing"

	"github.com/compose/transporter/client"
)

func TestReader(t *testing.T) {
	backchannel, db, err := Backchannel(TestUser, TestPass, TestURI)
	if err != nil {
		t.Fatal("failed to obtain backchannel")
	}

	CloudantAdaptor.Database = db.Name
	c, err := CloudantAdaptor.Client()
	testDocCount := 10
	if err != nil {
		t.Fatal("failed to start Cloudant Client")
	}

	s, err := c.Connect()
	done := make(chan struct{})
	defer func() {
		Tidy(backchannel, db.Name)
		c.(*Client).Close()
		close(done)
	}()

	if err != nil {
		t.Fatalf("unable to obtain Cloudant session, %s", err)
	}

	rd, err := CloudantAdaptor.Reader()
	if err != nil {
		t.Errorf("failed to start Cloudant Reader, %s", err)
	}

	// Make up some test data. Note that normal eventual consistency caveats
	// apply unless you run against a single node
	uploader := db.Bulk(testDocCount, 1024*1024, 0)
	uploader.BulkUploadSimple(MakeDocs(testDocCount))

	readFunc := rd.Read(map[string]client.MessageSet{}, func(ns string) bool {
		return true
	})

	changes, err := readFunc(s, done)
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
}
