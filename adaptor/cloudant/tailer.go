package cloudant

// The Cloudant Tailer uses the Follower functionality in the Cloudant
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
	_ client.Reader = &Tailer{}
	_ client.Closer = &Tailer{}
)

// Tailer fulfills the client.Reader interface for use with both copying and
// tailing a Cloudant database.
type Tailer struct {
	seqInterval int
	follower    *cdt.Follower
}

// Close is called by client.Close() when it receives on the done channel.
func (t *Tailer) Close() {
	t.follower.Close()
}

func newTailer(seqInterval int) client.Reader {
	return &Tailer{
		seqInterval: seqInterval,
	}
}

// Read a continuous changes feed
func (t *Tailer) Read(resumeMap map[string]client.MessageSet, filterFn client.NsFilterFunc) client.MessageChanFunc {
	return func(s client.Session, done chan struct{}) (chan client.MessageSet, error) {

		out := make(chan client.MessageSet)
		session := s.(*Session)
		t.follower = cdt.NewFollower(session.database, t.seqInterval)

		go func() {
			defer close(out)

			changes, err := t.follower.Follow()
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
						changes, err = t.follower.Follow()
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
					out <- client.MessageSet{Msg: msg}
				}
			}
		}()

		return out, nil
	}
}
