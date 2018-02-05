package cloudant

// The Cloudant Writer implementation

import (
	"fmt"

	cdt "github.com/cloudant-labs/go-cloudant"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/data"
	"github.com/compose/transporter/message/ops"
)

var _ client.Writer = &Writer{}

// Writer implements client.Writer
type Writer struct{}

func newWriter() *Writer {
	return &Writer{}
}

// Write inserts, updates or deletes a message
func (w *Writer) Write(msg message.Msg) func(client.Session) (message.Msg, error) {
	return func(s client.Session) (message.Msg, error) {
		db := s.(*Session).database
		var err error
		switch msg.OP() {
		case ops.Delete:
			err = deleteDoc(db, msg.Data())
		case ops.Update:
			err = updateDoc(db, msg.Data())
		default:
			err = insertDoc(db, msg.Data())
		}

		if err == nil && msg.Confirms() != nil {
			msg.Confirms() <- struct{}{}
		}

		return msg, err
	}
}

func insertDoc(database *cdt.Database, doc data.Data) error {
	_, err := database.Set(doc)
	return err
}

func updateDoc(database *cdt.Database, doc data.Data) error {
	_, hasID := doc.Has("_id")
	_, hasRev := doc.Has("_rev")
	if hasID && hasRev {
		return insertDoc(database, doc)
	}

	return fmt.Errorf("Document needs both _id and _rev to update")
}

func deleteDoc(database *cdt.Database, doc data.Data) error {
	_, hasID := doc.Has("_id")
	_, hasRev := doc.Has("_rev")
	if hasID && hasRev {
		err := database.Delete(stringField("_id", doc), stringField("_rev", doc))
		if err != nil {
			return err
		}
	}
	return fmt.Errorf("Document needs both _id and _rev to delete")
}

func stringField(key string, doc data.Data) string {
	switch val := doc.Get(key).(type) {
	case string:
		return val
	default:
		return fmt.Sprintf("%v", val)
	}
}
