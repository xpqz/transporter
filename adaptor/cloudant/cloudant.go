package cloudant

import (
	"sync"

	"github.com/compose/transporter/adaptor"
	"github.com/compose/transporter/client"
)

const (
	sampleConfig = `{
   "uri": "${CLOUDANT_URI}",
   //  "uri": "cloudant://...",
   //  "username": "username",
   //  "password": "password",
   //  "database": "database"
   //  "batchsize": int
   //  "timeout": int
   //  "seqInterval": int
}`

	description = "a Cloudant/CouchDB adaptor that functions as both source and sink"
)

var (
	_ adaptor.Adaptor = &cloudant{}
)

// Cloudant is an adaptor that reads and writes records to Cloudant (https://cloudant.com/)
type cloudant struct {
	adaptor.BaseConfig
	Tail        bool   `json:"tail"`
	Database    string `json:"database"`
	username    string
	password    string
	batchsize   int
	timeout     int
	seqInterval int
	cl          *Client
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
		WithUser(c.username),
		WithPassword(c.password),
		WithBatchSize(c.batchsize),
		WithTimeout(c.timeout),
		WithSeqInterval(c.seqInterval),
	)
	if err != nil {
		return nil, err
	}
	c.cl = cl
	return cl, nil
}

func (c *cloudant) Reader() (client.Reader, error) {
	return newReader(c.Tail, c.seqInterval), nil
}

func (c *cloudant) Writer(done chan struct{}, wg *sync.WaitGroup) (client.Writer, error) {
	return newWriter(c.cl.database, c.cl.batchsize, c.cl.timeout), nil
}

// Description for Cloudant adaptor
func (c *cloudant) Description() string {
	return description
}

// SampleConfig for Cloudant adaptor
func (c *cloudant) SampleConfig() string {
	return sampleConfig
}
