package cloudant

import (
	"fmt"
	"sync"

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
}`

	description = "a Cloudant/CouchDB adaptor that functions as both source and sink"
)

var (
	_ adaptor.Adaptor = &cloudant{}
)

// Cloudant is an adaptor that reads and writes records to Cloudant
// databases (https://cloudant.com/). Also compatible with CouchDB.
type cloudant struct {
	adaptor.BaseConfig
	Tail         bool   `json:"tail"`
	Database     string `json:"database"`
	Username     string `json:"username"`
	Password     string `json:"password"`
	BatchSize    int    `json:"batchsize"`
	BatchTimeout int    `json:"batchtimeout"`
	SeqInterval  int    `json:"seqinterval"`
	NewEdits     bool   `json:"newedits"`
	cl           *Client
}

func init() {
	adaptor.Add(
		"cloudant",
		func() adaptor.Adaptor {
			return &cloudant{NewEdits: true}
		},
	)
}

func (c *cloudant) Client() (client.Client, error) {
	cl, err := NewClient(
		WithURI(c.URI),
		WithDatabase(c.Database),
		WithUser(c.Username),
		WithPassword(c.Password),
		WithBatchSize(c.BatchSize),
		WithBatchTimeout(c.BatchTimeout),
		WithNewEdits(c.NewEdits),
		WithTail(c.Tail),
		WithSeqInterval(c.SeqInterval),
	)
	if err != nil {
		return nil, err
	}
	c.cl = cl

	return cl, nil
}

func (c *cloudant) Reader() (client.Reader, error) {
	return newReader(c.Tail, c.SeqInterval), nil
}

func (c *cloudant) Writer(done chan struct{}, wg *sync.WaitGroup) (client.Writer, error) {
	if c.cl.database == nil {
		return nil, fmt.Errorf("[Cloudant] no authenticated database")
	}
	return newWriter(c.cl.database, c.BatchSize, c.BatchTimeout, c.NewEdits), nil
}

// Description for Cloudant adaptor
func (c *cloudant) Description() string {
	return description
}

// SampleConfig for Cloudant adaptor
func (c *cloudant) SampleConfig() string {
	return sampleConfig
}
