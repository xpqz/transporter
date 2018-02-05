package cloudant

// The Cloudant Writer implementation

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	cdt "github.com/cloudant-labs/go-cloudant"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/data"
	"github.com/compose/transporter/message/ops"
)

var _ client.Writer = &Bulk{}

// Bulk implements client.Writer
type Bulk struct {
	buffer []interface{}
	*sync.RWMutex
	confirmChan  chan struct{}
	batchSize    int
	batchTimeout int
	newEdits     bool
}

func newBulker(done chan struct{}, wg *sync.WaitGroup, database *cdt.Database, batchSize int, batchTimeout int, newEdits bool) *Bulk {
	b := &Bulk{
		buffer:       make([]interface{}, batchSize),
		RWMutex:      &sync.RWMutex{},
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
		newEdits:     newEdits,
	}

	wg.Add(1)
	if batchTimeout <= 0 {
		b.batchTimeout = 2
	}
	go b.run(done, wg, database)

	return b
}

// Write queues the insert, update or delete of a message for future processing
func (b *Bulker) Write(msg message.Msg) func(client.Session) (message.Msg, error) {
	return func(s client.Session) (message.Msg, error) {
		b.Lock()
		b.confirmChan = msg.Confirms()

		db := s.(*Session).database
		var (
			err error
			doc data.Data
		)

		switch msg.OP() {
		case ops.Delete:
			doc, err = deleteDoc(msg.Data())
		case ops.Update:
			doc, err = updateDoc(msg.Data())
		default:
			doc = msg.Data()
		}

		if err != nil {
			b.buffer[len(b.buffer)] = doc
			if len(b.buffer) >= b.maxDoc {
				err = b.flush(db)
				if err != nil {
					log.With("database", db.Name).Errorf("bulk upload error, %s\n", err)
				} else if b.confirmChan != nil {
					b.confirmChan <- struct{}{}
				}
			}
		}
		b.Unlock()
		return msg, err
	}
}

func (b *Bulk) run(done chan struct{}, wg *sync.WaitGroup, database *cdt.Database) {
	defer wg.Done()
	for {
		select {
		case <-time.After(b.batchTimeout * time.Second):
			if err := b.drain(database); err != nil {
				log.Errorf("flush error, %s", err)
				return
			}
		case <-done:
			log.Debugln("received done channel")
			if err := b.drain(database); err != nil {
				log.Errorf("flush error, %s", err)
			}
			return
		}
	}
}

func (b *Bulk) drain(database *cdt.Database) error {
	b.Lock()
	err := b.flush(database)
	if err == nil {
		if b.confirmChan != nil {
			b.confirmChan <- struct{}{}
		}
	}
	b.Unlock()
	return err
}

func (b *Bulk) flush(database *cdt.Database) error {
	result, err := cdt.UploadBulkDocs(&cdt.BulkDocsRequest{Docs: b.buffer, NewEdits: b.newEdits}, database)
	defer result.Close()
	defer func() {
		b.buffer = make([]interface{}, b.batchSize)
	}()

	if err != nil {
		return err
	}

	if result == nil {
		return fmt.Errorf("batch upload yielded no result")
	}

	if result.response.StatusCode != 201 && result.response.StatusCode != 202 {
		return fmt.Errorf("unexpected HTTP return code, %d", result.response.StatusCode)
	}

	if result.response == nil {
		return fmt.Errorf("batch upload nil response")
	}

	responses := []cdt.BulkDocsResponse{}
	err = json.NewDecoder(result.response.Body).Decode(&responses)
	if err != nil {
		return err
	}

	return nil
}

func updateDoc(database *cdt.Database, doc data.Data) error {
	_, hasID := doc.Has("_id")
	_, hasRev := doc.Has("_rev")
	if hasID && hasRev {
		return doc, nil
	}

	return fmt.Errorf("Document needs both _id and _rev to update")
}

// deleteDoc verifies that the doc has both _id and _rev and then returns
// a new doc containing only _id, _rev, and _deleted: true which can be
// used by the Cloudant _bulk_docs endpoint to delete a doc as part of
// a larger batch
func deleteDoc(database *cdt.Database, doc data.Data) error {
	ID, hasID := doc.Has("_id")
	rev, hasRev := doc.Has("_rev")

	newDoc := &data.Data{}

	if hasID && hasRev {
		newDoc.Set("_id", ID)
		newDoc.Set("_rev", rev)
		newDoc.Set("_deleted", true)
		return newDoc, nil
	}
	return doc, fmt.Errorf("Document needs both _id and _rev to delete")
}
