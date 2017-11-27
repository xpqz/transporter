package cloudant

// The Cloudant Reader uses the Follower functionality in the Cloudant
// client library. It tails the changes feed continuously, and employs
// a few optimisations, such as increasing the spacing between sequence
// id generation.

import (
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
	cdt "github.ibm.com/cloudant/go-cloudant"
)

var (
	_ client.Reader = &Reader{}
)

// Reader fulfills the client.Reader interface for use with both copying and
// tailing a Cloudant database.
type Reader struct {
	tail bool
}

func newReader(tail bool) client.Reader {
	return &Reader{tail}
}

func (r *Reader) Read(_ map[string]client.MessageSet, filterFn client.NsFilterFunc) client.MessageChanFunc {
	return func(s client.Session, done chan struct{}) (chan client.MessageSet, error) {
		out := make(chan client.MessageSet)
		session := s.(*Session)
		follower := cdt.NewFollower(session.database)

		go func() {
			defer func() {
				close(out)
				follower.Close()
			}()

			changes, err := follower.Follow()
			if err != nil {
				log.With("db", session.dbName).
					With("err", err).
					Infoln("failed to open changes feed")
			}
			for {
				select {
				case <-done:
					log.With("db", session.dbName).
						Infoln("terminating normally")
					follower.Close()
					return
				case event := <-changes:
					var msg message.Msg
					switch event.EventType {
					case cdt.ChangesHeartbeat:
						// Deliberately left blank
					case cdt.ChangesError:
						log.With("db", session.dbName).
							With("err", event.Err).
							Infoln("changes error; skipping")
						continue
					case cdt.ChangesTerminated:
						log.With("db", session.dbName).
							Infoln("remote end terminated; resuming")
						changes, err = follower.Follow()
						if err != nil {
							log.With("db", session.dbName).
								With("err", err).
								Infoln("resumption error; giving up")
							return
						}
						continue
					case cdt.ChangesInsert:
						msg = message.From(ops.Insert, session.dbName, event.Doc)
						log.With("db", session.dbName).
							With("docid", event.Meta.ID).
							Infoln("INSERT")
					case cdt.ChangesDelete:
						msg = message.From(ops.Delete, session.dbName, event.Doc)
						log.With("db", session.dbName).
							With("docid", event.Meta.ID).
							Infoln("DELETE")
					default:
						msg = message.From(ops.Update, session.dbName, event.Doc)
						log.With("db", session.dbName).
							With("docid", event.Meta.ID).
							Infoln("UPDATE")
					}
					if filterFn(session.dbName) {
						out <- client.MessageSet{Msg: msg}
					}
				}
			}
		}()

		return out, nil
	}
}
