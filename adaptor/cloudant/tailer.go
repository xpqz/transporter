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

var _ client.Reader = &Tailer{}

// Reader implements client.Reader
type Tailer struct {
	seqInterval int
}

func newTailer(seqInterval int) *Tailer {
	return &Tailer{seqInterval}
}

// Read fulfils the Reader interface.
func (t *Tailer) Read(_ map[string]client.MessageSet, filterFn client.NsFilterFunc) client.MessageChanFunc {
	return func(s client.Session, done chan struct{}) (chan client.MessageSet, error) {
		out := make(chan client.MessageSet)
		session := s.(*Session)

		follower := cdt.NewFollower(db, t.seqInterval)
		changes, err := follower.Follow()
		if err != nil {
    		return nil, err
		}

		go func() {
			defer close(out)
			select <- done:
				return
			default:
				for {
					changeEvent := <-changes

					switch changeEvent.EventType {
					case cdt.ChangesHeartbeat:
						fmt.Println("tick")
					case cdt.ChangesError:
						fmt.Println(changeEvent.Err)
					case cdt.ChangesTerminated:
						fmt.Println("terminated; resuming from last known sequence id")
						changes, err = follower.Follow()
						if err != nil {
							fmt.Println("resumption error ", err)
							return
						}
					case cdt.ChangesInsert:
						fmt.Printf("INSERT %s\n", changeEvent.Meta.ID)
					case cdt.ChangesDelete:
						fmt.Printf("DELETE %s\n", changeEvent.Meta.ID)
					default:
						fmt.Printf("UPDATE %s\n", changeEvent.Meta.ID)
					}
				}
		}()

		query := cdt.NewChangesQuery().
			IncludeDocs().
			Feed("continuous")

		if t.seqInterval > 0 {
			query = query.SeqInterval(t.seqInterval)
		}

		changes, err := session.database.Changes(query.Build())
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
