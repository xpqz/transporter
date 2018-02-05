package cloudant

import (
	"sync"
	"testing"

	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
)

func TestBulkWrite(t *testing.T) {

	backchannel, db, err := Backchannel(TestUser, TestPass, TestURI)
	CloudantAdaptor.Database = db.Name
	CloudantAdaptor.BatchSize = 10
	CloudantAdaptor.BatchTimeout = 5
	CloudantAdaptor.NewEdits = true

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

	for i := 0; i < 105; i++ {
		wr.Write(message.From(ops.Insert, "bulk", map[string]interface{}{"foo": i, "i": i}))(s)
	}
	close(done)
	wg.Wait()
}
