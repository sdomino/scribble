// Package scribble is a tiny JSON database
package scribble

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/jcelliott/lumber"
)

// Version is the current version of the project
const Version = "1.0.4"

var (
	ErrMissingResource   = errors.New("missing resource - unable to save record")
	ErrMissingCollection = errors.New("missing collection - no place to save record")
)

type (
	// Logger is a generic logger interface
	Logger interface {
		Fatal(string, ...interface{})
		Error(string, ...interface{})
		Warn(string, ...interface{})
		Info(string, ...interface{})
		Debug(string, ...interface{})
		Trace(string, ...interface{})
	}

	// Driver is what is used to interact with the scribble database. It runs
	// transactions, and provides log output
	Driver struct {
		mutex   sync.Mutex
		mutexes map[string]*sync.Mutex
		dir     string // the directory where scribble will create the database
		log     Logger // the logger scribble will log to
	}
)

// Options uses for specification of working golang-scribble
type Options struct {
	Logger // the logger scribble will use (configurable)
}

// New creates a new scribble database at the desired directory location, and
// returns a *Driver to then use for interacting with the database
func New(dir string, options *Options) (*Driver, error) {

	//
	dir = filepath.Clean(dir)

	// create default options
	opts := Options{}

	// if options are passed in, use those
	if options != nil {
		opts = *options
	}

	// if no logger is provided, create a default
	if opts.Logger == nil {
		opts.Logger = lumber.NewConsoleLogger(lumber.INFO)
	}

	//
	driver := Driver{
		dir:     dir,
		mutexes: make(map[string]*sync.Mutex),
		log:     opts.Logger,
	}

	// if the database already exists, just use it
	if _, err := os.Stat(dir); err == nil {
		opts.Logger.Debug("Using '%s' (database already exists)\n", dir)
		return &driver, nil
	}

	// if the database doesn't exist create it
	opts.Logger.Debug("Creating scribble database at '%s'...\n", dir)
	return &driver, os.MkdirAll(dir, 0755)
}

// Write locks the database and attempts to write the record to the database under
// the [collection] specified with the [resource] name given
func (d *Driver) Write(collection, resource string, v interface{}) error {

	// ensure there is a place to save record
	if collection == "" {
		return ErrMissingCollection
	}

	// ensure there is a resource (name) to save record as
	if resource == "" {
		return ErrMissingResource
	}

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	//
	dir := filepath.Join(d.dir, collection)
	fnlPath := filepath.Join(dir, resource+".json")
	tmpPath := fnlPath + ".tmp"

	return write(dir, tmpPath, fnlPath, v)
}

func write(dir, tmpPath, dstPath string, v interface{}) error {

	// create collection directory
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// marshal the pointer to a non-struct and indent with tab
	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}

	// write marshaled data to the temp file
	if err := ioutil.WriteFile(tmpPath, b, 0644); err != nil {
		return err
	}

	// move final file into place
	return os.Rename(tmpPath, dstPath)
}

// Read a record from the database
func (d *Driver) Read(collection, resource string, v interface{}) error {

	// ensure there is a place to save record
	if collection == "" {
		return ErrMissingCollection
	}

	// ensure there is a resource (name) to save record as
	if resource == "" {
		return ErrMissingResource
	}

	//
	record := filepath.Join(d.dir, collection, resource)

	// read record from database; if the file doesn't exist `read` will return an err
	return read(record, v)
}

func read(record string, v interface{}) error {

	b, err := ioutil.ReadFile(record + ".json")
	if err != nil {
		return err
	}

	// unmarshal data
	return json.Unmarshal(b, &v)
}

// ReadAll records from a collection; this is returned as a slice of strings because
// there is no way of knowing what type the record is.
func (d *Driver) ReadAll(collection string) ([][]byte, error) {

	// ensure there is a collection to read
	if collection == "" {
		return nil, ErrMissingCollection
	}

	//
	dir := filepath.Join(d.dir, collection)

	// read all the files in the transaction.Collection; an error here just means
	// the collection is either empty or doesn't exist
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	return readAll(files, dir)
}

func readAll(files []os.FileInfo, dir string) ([][]byte, error) {
	// the files read from the database
	var records [][]byte

	// iterate over each of the files, attempting to read the file. If successful
	// append the files to the collection of read
	for _, file := range files {

		b, err := ioutil.ReadFile(filepath.Join(dir, file.Name()))

		if err != nil {
			return nil, err
		}

		// append read file
		records = append(records, b)
	}

	// unmarhsal the read files as a comma delimeted byte array
	return records, nil
}

// Delete locks the database then attempts to remove the collection/resource
// specified by [path]
func (d *Driver) Delete(collection, resource string) error {
	path := filepath.Join(collection, resource)
	//
	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	//
	dir := filepath.Join(d.dir, path)

	switch fi, err := stat(dir); {

	// if fi is nil or error is not nil return
	case fi == nil, err != nil:
		return fmt.Errorf("Unable to find file or directory named %v\n", path)

	// remove directory and all contents
	case fi.Mode().IsDir():
		return os.RemoveAll(dir)

	// remove file
	case fi.Mode().IsRegular():
		return os.RemoveAll(dir + ".json")
	}

	return nil
}

//
func stat(path string) (fi os.FileInfo, err error) {

	// check for dir, if path isn't a directory check to see if it's a file
	if fi, err = os.Stat(path); os.IsNotExist(err) {
		fi, err = os.Stat(path + ".json")
	}

	return
}

// getOrCreateMutex creates a new collection specific mutex any time a collection
// is being modified to avoid unsafe operations
func (d *Driver) getOrCreateMutex(collection string) *sync.Mutex {

	d.mutex.Lock()
	defer d.mutex.Unlock()

	m, ok := d.mutexes[collection]

	// if the mutex doesn't exist make it
	if !ok {
		m = &sync.Mutex{}
		d.mutexes[collection] = m
	}

	return m
}
