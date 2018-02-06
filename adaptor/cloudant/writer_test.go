package cloudant

import (
	"sync"
	"testing"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
)

func TestWriter(t *testing.T) {
	backchannel, db, err := Backchannel(TestUser, TestPass, TestURI)
	CloudantAdaptor.Database = db.Name
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
		close(done)
	}()

	if err != nil {
		t.Fatalf("unable to obtain session to cloudant: %s", err)
	}

	var wg sync.WaitGroup
	wr, err := CloudantAdaptor.Writer(done, &wg)
	if err != nil {
		t.Errorf("failed to start Cloudant Writer: %s", err)
	}

	confirms, cleanup := adaptor.MockConfirmWrites()
	defer adaptor.VerifyWriteConfirmed(cleanup, t)

	for i := 0; i < 10; i++ {
		msg := message.From(ops.Insert, "bulk", map[string]interface{}{"foo": i, "i": i})
		if _, err := wr.Write(message.WithConfirms(confirms, msg))(s); err != nil {
			t.Errorf("write error: %s", err)
		}
	}
}
