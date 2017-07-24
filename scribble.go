// Package scribble is a tiny JSON database
package scribble

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/jcelliott/lumber"
	"path"
	"strings"
	"strconv"
)

// Version is the current version of the project
const Version = "1.0.4"

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
		return fmt.Errorf("Missing collection - no place to save record!")
	}

	// ensure there is a resource (name) to save record as
	if resource == "" {
		return fmt.Errorf("Missing resource - unable to save record (no name)!")
	}

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	return d.writeFile(collection, resource, v)
}

func getCollectionDir(dir string, collection string) string {
	return filepath.Join(dir, collection)
}

// Writes a file to disk
func (d *Driver) writeFile(collection string, resource string, v interface{}) error {
	//
	dir := getCollectionDir(d.dir, collection)
	fnlPath := filepath.Join(dir, resource+".json")
	tmpPath := fnlPath + ".tmp"

	// create collection directory
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	//
	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}

	// write marshaled data to the temp file
	if err := ioutil.WriteFile(tmpPath, b, 0644); err != nil {
		return err
	}

	// move final file into place
	return os.Rename(tmpPath, fnlPath)
}

// Locks the database, gets the next available integer resource ID and attempts to write the
// record to the database under the [collection] specified with the generated [resource] ID
func (d *Driver) WriteAutoId(collection string, v interface{}) (resourceId int64, err error) {
	resourceId = -1

	// ensure there is a place to save record
	if collection == "" {
		return resourceId, fmt.Errorf("Missing collection - no place to save record!")
	}

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := getCollectionDir(d.dir, collection)

	// list the directory, sort it, take the last entry then parse and increment the last ID
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		if !os.IsNotExist(err) {
			fmt.Printf("Error listing directory %s: %s", dir, err.Error())
			return resourceId, err
		}
	}

	if len(files) == 0 {
		resourceId = 1
	} else {
		lastFile := files[len(files)-1]

		ext := path.Ext(lastFile.Name())

		baseName := strings.TrimSuffix(lastFile.Name(), ext)

		resourceId, err = strconv.ParseInt(baseName, 10, 8)
		if err != nil {
			fmt.Printf("Error parsing string '%s' as integer", baseName, err.Error())
			return resourceId, err
		}

		resourceId = resourceId + 1
	}

	stringResourceId := fmt.Sprintf("%08d", resourceId)

	fmt.Printf("Writing resource under auto-generated ID '%s'\n", stringResourceId)
	err = d.writeFile(collection, stringResourceId, v)

	return resourceId, err
}

// Read a record from the database
func (d *Driver) Read(collection, resource string, v interface{}) error {

	// ensure there is a place to save record
	if collection == "" {
		return fmt.Errorf("Missing collection - no place to save record!")
	}

	// ensure there is a resource (name) to save record as
	if resource == "" {
		return fmt.Errorf("Missing resource - unable to save record (no name)!")
	}

	//
	record := filepath.Join(d.dir, collection, resource)

	// check to see if file exists
	if _, err := stat(record); err != nil {
		return err
	}

	// read record from database
	b, err := ioutil.ReadFile(record + ".json")
	if err != nil {
		return err
	}

	// unmarshal data
	return json.Unmarshal(b, &v)
}

// ReadAll records from a collection; this is returned as a slice of strings because
// there is no way of knowing what type the record is.
func (d *Driver) ReadAll(collection string) ([]string, error) {

	// ensure there is a collection to read
	if collection == "" {
		return nil, fmt.Errorf("Missing collection - unable to record location!")
	}

	//
	dir := filepath.Join(d.dir, collection)

	// check to see if collection (directory) exists
	if _, err := stat(dir); err != nil {
		return nil, err
	}

	// read all the files in the transaction.Collection; an error here just means
	// the collection is either empty or doesn't exist
	files, _ := ioutil.ReadDir(dir)

	// the files read from the database
	var records []string

	// iterate over each of the files, attempting to read the file. If successful
	// append the files to the collection of read files
	for _, file := range files {
		b, err := ioutil.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			return nil, err
		}

		// append read file
		records = append(records, string(b))
	}

	// unmarhsal the read files as a comma delimeted byte array
	return records, nil
}

// Delete locks that database and then attempts to remove the collection/resource
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
// is being modfied to avoid unsafe operations
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
