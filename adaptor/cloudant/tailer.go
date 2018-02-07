package cloudant

// The Cloudant Reader uses the Follower functionality in the go-cloudant
// client library in order to tail a continuous changes feed.

import (
	cdt "github.com/cloudant-labs/go-cloudant"
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/commitlog"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"
)

var _ client.Reader = &Tailer{}

// Tailer implements client.Reader
type Tailer struct {
	seqInterval int
}

func newTailer(seqInterval int) *Tailer {
	return &Tailer{seqInterval}
}

// Read tails a continuous changes feed
func (t *Tailer) Read(_ map[string]client.MessageSet, filterFn client.NsFilterFunc) client.MessageChanFunc {
	return func(s client.Session, done chan struct{}) (chan client.MessageSet, error) {
		out := make(chan client.MessageSet)
		session := s.(*Session)
		follower := cdt.NewFollower(session.database, t.seqInterval)

		go func() {
			defer close(out)
			defer follower.Close()

			changes, err := follower.Follow()
			if err != nil {
				log.With("db", session.dbName).
					With("err", err).
					Infoln("failed to open changes feed")
				return
			}
			for {
				select {
				case <-done:
					log.With("db", session.dbName).
						Infoln("terminating normally")
					return
				case event := <-changes:
					var msg message.Msg
					switch event.EventType {
					case cdt.ChangesHeartbeat:
						continue
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
					case cdt.ChangesDelete:
						msg = message.From(ops.Delete, session.dbName, event.Doc)
					default:
						msg = message.From(ops.Update, session.dbName, event.Doc)
					}
					out <- client.MessageSet{
						Msg:  msg,
						Mode: commitlog.Sync,
					}
				}
			}
		}()

		return out, nil
	}
}
