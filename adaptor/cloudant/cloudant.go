package cloudant

import (
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
		// Note: all cloudant URIs must be "cloudant://..."
}`

	description = "a Cloudant adaptor that functions as both source and sink"
)

// cloudant implements adaptor.Adaptor
var _ adaptor.Adaptor = &cloudant{}

// Cloudant is an adaptor that reads and writes records to Cloudant databases
type cloudant struct {
	adaptor.BaseConfig
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
	cl       *Client
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
		WithPassword(c.Password),
	)
	if err != nil {
		return nil, err
	}
	c.cl = cl

	return cl, nil
}

func (c *cloudant) Reader() (client.Reader, error) {
	return newReader(), nil
}

func (c *cloudant) Writer(done chan struct{}, wg *sync.WaitGroup) (client.Writer, error) {
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
