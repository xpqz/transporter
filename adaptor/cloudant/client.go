package cloudant

import (
	"net/url"

	cdt "github.com/cloudant-labs/go-cloudant"
	"github.com/compose/transporter/client"
)

const (
	// DefaultURI is the default endpoint for CouchDB on the local machine.
	DefaultURI = "cloudant://127.0.0.1:5984"
)

// Client implements client.Client and client.Closer
var (
	_ client.Client = &Client{}
	_ client.Closer = &Client{}
)

// Client creates and holds the session to Cloudant
type Client struct {
	client   *cdt.CouchClient
	database *cdt.Database
	bulker   *cdt.Uploader
	dbName   string
	uri      string
	username string
	password string
}

// Session wraps the access points for consumption by Reader and Writer
type Session struct {
	client   *cdt.CouchClient
	dbName   string
	database *cdt.Database
}

// ClientOptionFunc is a function that configures a Client.
type ClientOptionFunc func(*Client) error

// NewClient creates a Cloudant client
func NewClient(options ...ClientOptionFunc) (*Client, error) {
	c := &Client{uri: DefaultURI}

	for _, option := range options {
		if err := option(c); err != nil {
			return nil, err
		}
	}

	if err := c.initConnection(); err != nil {
		return nil, err
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

	// Select the database, creating it if it's not there: both source
	// and sink uses this.
	database, err := cl.GetOrCreate(c.dbName)
	if err != nil {
		return client.ConnectError{Reason: err.Error()}
	}
	c.database = database

	return nil
}
