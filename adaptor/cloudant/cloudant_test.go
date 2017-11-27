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
	"sync"
	"testing"
	"time"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
)

var (
	cloudantAdaptor = &cloudant{
		BaseConfig: adaptor.BaseConfig{URI: DefaultURI},
		Database:   "testdb",
		username:   "admin",
		password:   "xyzzy",
		batchsize:  10,
		timeout:    3,
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
	c, err := cloudantAdaptor.Client()
	if err != nil {
		t.Fatal("Failed to start Cloudant Client")
	}

	// defer c.Close()
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to obtain session to cloudant, %s", err)
	}

	done := make(chan struct{})
	var wg sync.WaitGroup

	wr, err := cloudantAdaptor.Writer(done, &wg)
	if err != nil {
		t.Errorf("Failed to start Cloudant Writer, %s", err)
	}

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
	time.Sleep(3 * time.Second)
	close(done)
}
