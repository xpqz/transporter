package cloudant

import (
	"testing"

	"github.com/compose/transporter/client"
)

func TestTailerDoneTerminates(t *testing.T) {
	backchannel, db, err := Backchannel(TestUser, TestPass, TestURI)
	if err != nil {
		t.Fatal("failed to obtain backchannel")
	}

	CloudantAdaptor.Database = db.Name
	CloudantAdaptor.Tail = true

	c, err := CloudantAdaptor.Client()
	testDocCount := 10
	if err != nil {
		t.Fatal("failed to start Cloudant Client")
	}

	s, err := c.Connect()
	defer func() {
		Tidy(backchannel, db.Name)
		c.(*Client).Close()
	}()

	if err != nil {
		t.Fatalf("unable to obtain Cloudant session, %s", err)
	}

	tailer, err := CloudantAdaptor.Reader()
	if err != nil {
		t.Errorf("failed to start Cloudant Tailing Reader, %s", err)
	}

	// Make up some test data. Note that normal eventual consistency caveats
	// apply unless you run against a single node
	uploader := db.Bulk(testDocCount, 1024*1024, 0)
	uploader.BulkUploadSimple(MakeDocs(testDocCount))

	readFunc := tailer.Read(map[string]client.MessageSet{}, func(ns string) bool {
		return true
	})

	done := make(chan struct{})
	changes, err := readFunc(s, done)
	if err != nil {
		t.Fatalf("unexpected Read error, %s\n", err)
	}
	var numMsgs int
	for _ = range changes {
		numMsgs++
		if numMsgs == testDocCount {
			close(done)
		}
	}
}
