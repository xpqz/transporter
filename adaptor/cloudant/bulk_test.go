package cloudant

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
)

func TestBulkWriteDrainsOnDone(t *testing.T) {
	testDocCount := 105
	backchannel, db, err := Backchannel(TestUser, TestPass, TestURI)
	CloudantAdaptor.Database = db.Name
	CloudantAdaptor.BatchSize = 10
	CloudantAdaptor.BatchTimeout = 5

	c, err := CloudantAdaptor.Client()
	if err != nil {
		t.Fatal("failed to start Cloudant Client")
	}

	s, err := c.Connect()
	done := make(chan struct{})
	defer func() {
		Tidy(backchannel, db.Name)
		c.(*Client).Close()
	}()

	if err != nil {
		t.Fatalf("unable to obtain session to cloudant: %s", err)
	}

	var wg sync.WaitGroup
	wr, err := CloudantAdaptor.Writer(done, &wg)
	if err != nil {
		t.Errorf("failed to start Cloudant Bulk Writer: %s", err)
	}

	for i := 0; i < testDocCount; i++ {
		wr.Write(message.From(ops.Insert, "bulk", map[string]interface{}{
			"foo": i,
			"i":   i,
		}))(s)
	}
	close(done)
	wg.Wait()

	// Sink DB should have testDocCount docs
	docs := AllDocs(db)
	if len(docs) != testDocCount {
		t.Errorf("found %d docs, expected %d", len(docs), testDocCount)
	}
}

func TestBulkMixedOps(t *testing.T) {
	testDocCount := 105
	backchannel, db, err := Backchannel(TestUser, TestPass, TestURI)
	CloudantAdaptor.Database = db.Name
	CloudantAdaptor.BatchSize = 10
	CloudantAdaptor.BatchTimeout = 1

	c, err := CloudantAdaptor.Client()
	if err != nil {
		t.Fatal("failed to start Cloudant Client")
	}

	s, err := c.Connect()
	done := make(chan struct{})
	defer func() {
		Tidy(backchannel, db.Name)
		c.(*Client).Close()
	}()

	if err != nil {
		t.Fatalf("unable to obtain session to cloudant: %s", err)
	}

	var wg sync.WaitGroup
	wr, err := CloudantAdaptor.Writer(done, &wg)
	if err != nil {
		t.Errorf("failed to start Cloudant Bulk Writer: %s", err)
	}

	for i := 0; i < testDocCount; i++ {
		wr.Write(message.From(ops.Insert, "bulk", map[string]interface{}{
			"foo": i,
			"i":   i,
		}))(s)
	}

	time.Sleep(1100 * time.Millisecond)

	docs := AllDocs(db)
	expectedDocCount := 0
	for index, item := range docs {
		if index%2 == 0 {
			expectedDocCount++
			wr.Write(message.From(ops.Update, "bulk", map[string]interface{}{
				"_id":  item.ID,
				"_rev": item.Rev,
				"foo":  index,
				"i":    index + 2,
			}))(s)
		} else {
			wr.Write(message.From(ops.Delete, "bulk", map[string]interface{}{
				"_id":  item.ID,
				"_rev": item.Rev,
			}))(s)
		}
	}

	close(done)
	wg.Wait()

	docs = AllDocs(db)
	for _, row := range docs {
		if !strings.HasPrefix(row.Rev, "2-") {
			t.Errorf("found unexpected rev, %s", row.Rev)
		}
	}

	if len(docs) != expectedDocCount {
		t.Errorf("found unexpected doc count, %d", len(docs))
	}
}

func TestBulkNoNewEditsRetainsRevs(t *testing.T) {
	testDocCount := 105
	backchannel, db, err := Backchannel(TestUser, TestPass, TestURI)
	CloudantAdaptor.Database = db.Name
	CloudantAdaptor.BatchSize = 10
	CloudantAdaptor.BatchTimeout = 5
	CloudantAdaptor.NewEdits = false // replicator mode

	c, err := CloudantAdaptor.Client()
	if err != nil {
		t.Fatal("failed to start Cloudant Client")
	}

	s, err := c.Connect()
	done := make(chan struct{})
	defer func() {
		Tidy(backchannel, db.Name)
		c.(*Client).Close()
	}()

	if err != nil {
		t.Fatalf("unable to obtain session to cloudant: %s", err)
	}

	var wg sync.WaitGroup
	wr, err := CloudantAdaptor.Writer(done, &wg)
	if err != nil {
		t.Errorf("failed to start Cloudant Bulk Writer: %s", err)
	}

	for i := 0; i < testDocCount; i++ {
		wr.Write(message.From(ops.Insert, "bulk", map[string]interface{}{
			"_id":  RevOrIDIsh(),
			"_rev": fmt.Sprintf("5-%s", RevOrIDIsh()),
			"foo":  i,
			"i":    i,
		}))(s)
	}
	close(done)
	wg.Wait()

	docs := AllDocs(db)
	for _, row := range docs {
		if !strings.HasPrefix(row.Rev, "5-") {
			t.Errorf("found unexpected rev, %s", row.Rev)
		}
	}
}
