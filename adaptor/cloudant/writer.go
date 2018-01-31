package cloudant

// The Cloudant Writer uses the Uploader functionality in the Cloudant
// client library. It spreads uploads across multiple go routines and
// utilises the efficient _bulk_docs API endpoint to batch uploads. It
// also sets a timeout after which the queue is drained regardless of
// the number of items present.

import (
	cdt "github.com/cloudant-labs/go-cloudant"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
)

var (
	_ client.Writer = &Writer{}
	_ client.Closer = &Writer{}
)

const bufferSize = 1024 * 1024 // Cloudant has a max request size of 1M

// Writer implements client.Writer for use with Cloudant
type Writer struct {
	bulker *cdt.Uploader
}

func newWriter(db *cdt.Database, batchSize int, batchTimeout int, newEdits bool) *Writer {
	log.With("db", db.Name).
		With("batchsize", batchSize).
		With("batchTimeout", batchTimeout).
		Infoln("BULKER STARTING")
	bulker := db.Bulk(batchSize, bufferSize, batchTimeout)
	// NewEdits == false is a signal to the bulker to run in replicator mode,
	// forcing documents even when they result in conflicts. This is required
	// in order to preserve _revs coming from a Cloudant source if running
	// transporter Cloudant => Cloudant. The default is true, meaning that _revs
	// are generated, rather than preserved.
	if newEdits == false {
		bulker.NewEdits = false
	}
	return &Writer{bulker}
}

// Close is called by client.Close() when it receives on the done channel.
func (w *Writer) Close() {
	w.bulker.Stop()
}

// Write adds a message to the upload queues. These will be uploaded when full,
// or if the max allowable time since the previous upload is exceeded.
func (w *Writer) Write(msg message.Msg) func(client.Session) (message.Msg, error) {
	return func(s client.Session) (message.Msg, error) {
		w.bulker.Upload(msg.Data())
		return msg, nil
	}
}
