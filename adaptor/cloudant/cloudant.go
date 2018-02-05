package cloudant

import (
	"sync"
	"time"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/client"
)

const (
	sampleConfig = `{
        "uri": "${CLOUDANT_URI}",
        "username": "username",
        "password": "password",
		"database": "database",
		//  "batchsize": int,         // Sink only
		//  "batchtimeout": int,      // Sink only
		//  "seqinterval": int,       // Source only
		//  "tail": bool,             // Source only
		//  "newedits": bool,         // Sink only
		// Note: all cloudant URIs must be "cloudant://..."
}`

	description = "a Cloudant adaptor that functions as both source and sink"
)

// cloudant implements adaptor.Adaptor
var _ adaptor.Adaptor = &cloudant{}

const bufferSize = 1024 * 1024 // Cloudant has a max request size of 1M

// Cloudant is an adaptor that reads and writes records to Cloudant databases
type cloudant struct {
	adaptor.BaseConfig
	Database     string        `json:"database"`
	Username     string        `json:"username"`
	Password     string        `json:"password"`
	BatchSize    int           `json:"batchsize"`
	BatchTimeout time.Duration `json:"batchtimeout"`
	SeqInterval  int           `json:"seqinterval"`
	NewEdits     bool          `json:"newedits"`
	Tail         bool          `json:"tail"`
	cl           *Client
}

func init() {
	adaptor.Add(
		"cloudant",
		func() adaptor.Adaptor {
			return &cloudant{}
		},
	)
}

func (c *cloudant) Client() (client.Client, error) {
	cl, err := NewClient(
		WithURI(c.URI),
		WithDatabase(c.Database),
		WithUser(c.Username),
		WithBatchSize(c.BatchSize),
		WithBatchTimeout(c.BatchTimeout),
		WithNewEdits(c.NewEdits),
		WithSeqInterval(c.SeqInterval),
		WithPassword(c.Password),
	)
	if err != nil {
		return nil, err
	}
	c.cl = cl

	return cl, nil
}

func (c *cloudant) Reader() (client.Reader, error) {
	if c.Tail {
		return newTailer(c.SeqInterval), nil
	}
	return newReader(), nil
}

func (c *cloudant) Writer(done chan struct{}, wg *sync.WaitGroup) (client.Writer, error) {
	if c.BatchSize > 0 {
		return newBulker(done, wg, c.cl.database, c.BatchSize, c.BatchTimeout, c.NewEdits), nil
	}
	return newWriter(), nil
}

// Description for Cloudant adaptor
func (c *cloudant) Description() string {
	return description
}

// SampleConfig for Cloudant adaptor
func (c *cloudant) SampleConfig() string {
	return sampleConfig
}
