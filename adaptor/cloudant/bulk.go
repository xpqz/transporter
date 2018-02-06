package cloudant

// The Cloudant Bulk Writer implementation.
//
// This writer uses a configurable buffer and the Cloudant _bulk_docs API.
// It is the most efficient way of loading larger document counts into
// a Cloudant database.

import (
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

var _ client.Writer = &Bulker{}

// Bulker implements client.Writer
type Bulker struct {
	buffer []interface{}
	*sync.RWMutex
	confirmChan  chan struct{}
	batchSize    int
	batchTimeout time.Duration
	newEdits     bool
}

func newBulker(done chan struct{}, wg *sync.WaitGroup, db *cdt.Database, size int, dur time.Duration, newEdits bool) *Bulker {
	b := &Bulker{
		buffer:       []interface{}{},
		RWMutex:      &sync.RWMutex{},
		batchSize:    size,
		batchTimeout: dur,
		newEdits:     newEdits,
	}

	wg.Add(1)
	if dur <= 0 {
		b.batchTimeout = 2
	}
	go b.run(done, wg, db)

	return b
}

// Write queues the insert, update or delete of a message for future processing.
// If the buffer has reached its max occupancy, we trigger a flush.
func (b *Bulker) Write(msg message.Msg) func(client.Session) (message.Msg, error) {
	return func(s client.Session) (message.Msg, error) {
		b.Lock()
		b.confirmChan = msg.Confirms()
		db := s.(*Session).database
		doc, err := isBulkable(msg)

		if err == nil {
			b.buffer = append(b.buffer, doc)
			if len(b.buffer) >= b.batchSize {
				err = b.flush(db)
				if err == nil && b.confirmChan != nil {
					b.confirmChan <- struct{}{}
				}
			}
		}
		b.Unlock()
		return msg, err
	}
}

func (b *Bulker) run(done chan struct{}, wg *sync.WaitGroup, database *cdt.Database) {
	defer wg.Done()
	for {
		select {
		case <-time.After(b.batchTimeout * time.Second):
			log.Debugln("draining upload buffer on time interval")
			if err := b.drain(database); err != nil {
				log.Errorf("time interval drain error, %s", err)
				return
			}

		case <-done:
			log.Debugln("draining upload buffer: received on done channel")
			if err := b.drain(database); err != nil {
				log.Errorf("done channel drain error, %s", err)
			}
			return
		}
	}
}

func (b *Bulker) drain(database *cdt.Database) error {
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

func (b *Bulker) flush(database *cdt.Database) error {
	result, err := cdt.UploadBulkDocs(&cdt.BulkDocsRequest{Docs: b.buffer, NewEdits: b.newEdits}, database)
	defer func() {
		result.Close()
		b.buffer = []interface{}{} // Recycle the buffer, as this batch is done
	}()

	if err != nil {
		return err
	}

	// Check that we get a 2XX response, but Transporter need not care about
	// the actual response body
	response := result.Response()
	if response.StatusCode != 201 && response.StatusCode != 202 {
		return fmt.Errorf("unexpected HTTP return code, %d", response.StatusCode)
	}

	return nil
}

func isBulkable(msg message.Msg) (data.Data, error) {
	doc := msg.Data()

	ID, hasID := doc.Has("_id")
	rev, hasRev := doc.Has("_rev")

	op := msg.OP()
	if op == ops.Delete || op == ops.Update {
		if !hasID || !hasRev {
			return doc, fmt.Errorf("Document needs both _id and _rev")
		}
	}

	if op == ops.Delete {
		newDoc := data.Data{}
		newDoc.Set("_id", ID)
		newDoc.Set("_rev", rev)
		newDoc.Set("_deleted", true)
		return newDoc, nil
	}

	return doc, nil
}
