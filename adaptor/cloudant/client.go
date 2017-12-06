package cloudant

import (
	"net/url"

	"github.com/compose/transporter/client"
	cdt "github.ibm.com/cloudant/go-cloudant"
)

const (
	// DefaultURI is the default endpoint for CouchDB on the local machine.
	// Primarily used when initializing a new Client without a specific URI.
	DefaultURI         = "cloudant://127.0.0.1:5984"
	uploadBatchSize    = 500
	uploadQueueTimeout = 5
)

var ( // Ensure that cloudant.Client implements Client and Closer
	_ client.Client = &Client{}
	_ client.Closer = &Client{}
)

// Client creates and holds the session to Cloudant
type Client struct {
	client      *cdt.CouchClient
	database    *cdt.Database
	dbName      string
	uri         string
	username    string
	password    string
	batchsize   int // sink-only
	timeout     int // sink-only
	seqInterval int // source only
}

// Session wraps the access points for consumption by Reader and Writer
type Session struct {
	client   *cdt.CouchClient
	dbName   string
	database *cdt.Database
}

// ClientOptionFunc is a function that configures a Client.
// It is used in NewClient.
type ClientOptionFunc func(*Client) error

// NewClient creates a new client to work with Cloudant.
//
// The caller can configure the new client by passing configuration options
// to the func.
//
// Example:
//
//   client, err := NewClient(
//     WithURI("cloudant://account.cloudant.com"),
//     WithDatabase("shopping-list"),
//     WithUser("account"),
//     WithPassword("password"))
//
// If no URI is configured, it uses DefaultURI.
//
// An error is also returned when some configuration option is invalid
func NewClient(options ...ClientOptionFunc) (*Client, error) {
	// Set up the client
	c := &Client{
		uri: DefaultURI,
	}

	// Run the options on it
	for _, option := range options {
		if err := option(c); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// WithURI defines the full connection string of the Cloudant database.
func WithURI(uri string) ClientOptionFunc {
	return func(c *Client) error {
		_, err := url.Parse(c.uri)
		if err != nil {
			return client.InvalidURIError{URI: uri, Err: err.Error()}
		}
		c.uri = uri
		return nil
	}
}

// WithBatchSize defines the number of docs to cache before a bulk-write
func WithBatchSize(batchsize int) ClientOptionFunc {
	return func(c *Client) error {
		c.batchsize = batchsize
		return nil
	}
}

// WithTimeout defines the max number of seconds to hold the write cache
// regardless of the number of items
func WithTimeout(secs int) ClientOptionFunc {
	return func(c *Client) error {
		c.timeout = secs
		return nil
	}
}

// WithSeqInterval defines the frequency of sequence id generation in
// the changes feed. Setting this to a number > 0 vastly improves performance
// at the cost of coarser granularity in terms of resumption.
func WithSeqInterval(interval int) ClientOptionFunc {
	return func(c *Client) error {
		c.seqInterval = interval
		return nil
	}
}

// WithUser defines the username
func WithUser(name string) ClientOptionFunc {
	return func(c *Client) error {
		c.username = name
		return nil
	}
}

// WithPassword defines the password
func WithPassword(pass string) ClientOptionFunc {
	return func(c *Client) error {
		c.password = pass
		return nil
	}
}

// WithDatabase defines the database
func WithDatabase(db string) ClientOptionFunc {
	return func(c *Client) error {
		c.dbName = db
		return nil
	}
}

// Connect wraps the underlying session to the Cloudant database
func (c *Client) Connect() (client.Session, error) {
	if c.database == nil {
		if err := c.initConnection(); err != nil {
			return nil, err
		}
	}
	return &Session{
		client:   c.client,
		dbName:   c.dbName,
		database: c.database,
	}, nil
}

// Close fulfills the Closer interface and takes care of cleaning up the session
func (c Client) Close() {
	if c.database != nil {
		c.client.LogOut()
		c.client.Stop()
	}
}

func (c *Client) initConnection() error {
	uri, _ := url.Parse(c.uri)

	// If we don't have a database already, try to grab it from the URI
	if c.dbName == "" {
		c.dbName = uri.Path[1:]
	}

	// If we don't have credentials, try to take them from the URI
	if c.username == "" {
		if pwd, ok := uri.User.Password(); ok {
			c.username = uri.User.Username()
			c.password = pwd
		}
	}

	// Clean up the URI so we can use it as the input to the Cloudant client
	if c.uri == DefaultURI {
		uri.Scheme = "http" // Local CouchDB
	} else {
		uri.Scheme = "https"
	}
	uri.User = nil

	// Authenticate
	cl, err := cdt.CreateClient(c.username, c.password, uri.String(), 5)
	if err != nil {
		return client.ConnectError{Reason: err.Error()}
	}
	c.client = cl

	// Select the database
	database, err := cl.Get(c.dbName)
	if err != nil {
		return client.ConnectError{Reason: err.Error()}
	}
	c.database = database

	return nil
}
