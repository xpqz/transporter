package cloudant

// The Cloudant Writer uses the Uploader functionality in the Cloudant
// client library. It spreads uploads across multiple go routines and
// utilises the efficient _bulk_docs API endpoint to batch uploads. It
// also sets a timeout after which the queue is drained regardless of
// the number of items present.

import (
	cdt "github.com/cloudant-labs/go-cloudant"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/message"
)

var (
	_ client.Writer = &Writer{}
	_ client.Closer = &Writer{}
)

const (
	bufferSize = 1024 * 1024 // Cloudant has a max request size of 1M
	batchSize  = 200
	queueTTL   = 5
)

// Writer implements client.Writer for use with Cloudant
type Writer struct {
	bulker *cdt.Uploader
}

func newWriter(db *cdt.Database) *Writer {
	return &Writer{db.Bulk(batchSize, bufferSize, queueTTL)}
}

// Close is called by client.Close() when it receives on the done channel.
func (w *Writer) Close() {
	w.bulker.Stop()
}

// Write adds a message to the upload queues.
func (w *Writer) Write(msg message.Msg) func(client.Session) (message.Msg, error) {
	return func(s client.Session) (message.Msg, error) {
		w.bulker.Upload(msg.Data())
		return msg, nil
	}
}
