package cloudant

// The Cloudant Reader uses the Changes() functionality in the Cloudant
// client library.

import (
	"strings"

	cdt "github.com/cloudant-labs/go-cloudant"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
)

var _ client.Reader = &Reader{}

// Reader implements client.Reader
type Reader struct{}

func newReader() client.Reader {
	return &Reader{}
}

// Read fulfils the Reader interface.
func (r *Reader) Read(_ map[string]client.MessageSet, filterFn client.NsFilterFunc) client.MessageChanFunc {
	return func(s client.Session, done chan struct{}) (chan client.MessageSet, error) {
		out := make(chan client.MessageSet)
		session := s.(*Session)

		query := cdt.NewChangesQuery().
			IncludeDocs().
			Build()

		changes, err := session.database.Changes(query)
		if err != nil {
			return nil, err
		}

		go func() {
			defer close(out)

			for {
				select {
				case <-done:
					return
				case change, more := <-changes:
					if !more {
						return
					}
					if change != nil && filterFn(session.dbName) {
						out <- client.MessageSet{Msg: makeMessage(change, session.dbName)}
					}
				}
			}
		}()

		return out, nil
	}
}

func makeMessage(change *cdt.Change, db string) message.Msg {
	if change.Deleted {
		return message.From(ops.Delete, db, change.Doc)
	}

	if strings.HasPrefix(change.Rev, "1-") {
		return message.From(ops.Insert, db, change.Doc)
	}

	return message.From(ops.Update, db, change.Doc)
}
