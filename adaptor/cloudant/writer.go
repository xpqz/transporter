package cloudant

// The Cloudant Writer implementation

import (
	"github.com/compose/transporter/client"
	"github.com/compose/transporter/message"
)

var _ client.Writer = &Writer{}

// Writer implements client.Writer
type Writer struct{}

func newWriter() *Writer {
	return &Writer{}
}

// Write inserts a message
func (w *Writer) Write(msg message.Msg) func(client.Session) (message.Msg, error) {
	return func(s client.Session) (message.Msg, error) {
		_, err := s.(*Session).database.Set(msg.Data())
		return msg, err
	}
}
