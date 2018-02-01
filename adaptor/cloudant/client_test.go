package cloudant

// This contains utility routines for the test suite.
//
// Assume local couchdb is running on localhost:5984
//
// To run: go test -v ./adaptor/cloudant/...
//
// To run tests against a local CouchDB we need to ensure
// that we disable CouchDB's admin party mode by creating
// an admin user, as the go-cloudant library does not allow
// unauthenticated connections.
//
// % docker run -d -p 5984:5984 --rm --name couchdb couchdb:1.6
// % export HOST="http://127.0.0.1:5984"
// % curl -XPUT $HOST/_config/admins/admin -d '"xyzzy"'
// % curl -XPUT $HOST/testdb -u admin

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"
	"time"

	cdt "github.com/cloudant-labs/go-cloudant"
	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/client"
)

const (
	TestUser = "admin"
	TestPass = "xyzzy"
	TestURI  = "http://127.0.0.1:5984"
)

var (
	CloudantAdaptor = &cloudant{
		BaseConfig: adaptor.BaseConfig{URI: DefaultURI},
		Username:   TestUser,
		Password:   TestPass,
	}
)

// This is rather crude -- expect problems if testing against a non-local
// cluster.
func CheckCount(desc string, expected int, msgChan <-chan client.MessageSet, t *testing.T) {
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

func MakeDocs(docCount int) []interface{} {
	// Insert 10 docs
	docs := make([]interface{}, docCount)
	for i := 0; i < docCount; i++ {
		docs[i] = struct {
			Foo string `json:"foo"`
			Bar string `json:"bar"`
		}{
			UUIDIsh(),
			UUIDIsh(),
		}
	}

	return docs
}

func UUIDIsh() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return ""
	}

	uuid := fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])

	return uuid
}

func DbName() string {
	return fmt.Sprintf("transporter-%x", sha256.Sum256([]byte(UUIDIsh())))
}

func Tidy(cl *cdt.CouchClient, dbName string) {
	cl.Delete(dbName)
	cl.LogOut()
	cl.Stop()
}

func Backchannel(username, password, baseURL string) (*cdt.CouchClient, *cdt.Database, error) {
	client, err := cdt.CreateClient(username, password, baseURL, 1)
	if err != nil {
		return nil, nil, err
	}
	database, err := client.GetOrCreate(DbName())
	if err != nil {
		return nil, nil, err
	}

	return client, database, nil
}
