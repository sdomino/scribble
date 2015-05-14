package scribble

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"

	"github.com/pagodabox/golang-hatchet"
)

//
const Version = "0.0.1"

//
type (

	// Driver
	Driver struct {
		mutexes map[string]sync.Mutex
		dir      string
		log      hatchet.Logger
	}

	// Transaction represents
	Transaction struct {
		Action     string
		Collection string
		ResourceID string
		Container  interface{}
	}
)

// New
func New(dir string, logger hatchet.Logger) (*Driver, error) {
	fmt.Printf("Creating database directory at '%v'...\n", dir)

	//
  if logger == nil {
    logger = hatchet.DevNullLogger{}
  }

	//
	scribble := &Driver{
		dir: 		 dir,
		mutexes: make(map[string]sync.Mutex),
		log: 		 logger,
	}

	//
	if err := mkDir(scribble.dir); err != nil {
		return nil, err
	}

	//
	return scribble, nil
}

// Transact
func (d *Driver) Transact(trans Transaction) error {

	//
	switch trans.Action {
	case "write":
		return d.write(trans)
	case "read":
		return d.read(trans)
	case "readall":
		return d.readAll(trans)
	case "delete":
		return d.delete(trans)
	default:
		return errors.New(fmt.Sprintf("Unsupported action %+v", trans.Action))
	}

	return nil
}

// private

// write
func (d *Driver) write(trans Transaction) error {

	mutex := d.getOrCreateMutex(trans.Collection)
	mutex.Lock()

	//
	dir := d.dir + "/" + trans.Collection

	//
	b, err := json.MarshalIndent(trans.Container, "", "\t")
	if err != nil {
		return err
	}

	//
	if err := mkDir(dir); err != nil {
		return err
	}

	//
	if err := ioutil.WriteFile(dir + "/" + trans.ResourceID, b, 0666); err != nil {
		return err
	}

	mutex.Unlock()

	return nil
}

// read
func (d *Driver) read(trans Transaction) error {

	dir := d.dir + "/" + trans.Collection

	b, err := ioutil.ReadFile(dir + "/" + trans.ResourceID)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(b, trans.Container); err != nil {
		return err
	}

	return nil
}

// readAll
func (d *Driver) readAll(trans Transaction) error {

	dir := d.dir + "/" + trans.Collection

	//
	files, err := ioutil.ReadDir(dir)

	// an error here just means an empty collection so do nothing
	if err != nil {
	}

	var f []string

	for _, file := range files {
		b, err := ioutil.ReadFile(dir + "/" + file.Name())
		if err != nil {
			return err
		}

		f = append(f, string(b))
	}

	//
	if err := json.Unmarshal([]byte("["+strings.Join(f, ",")+"]"), trans.Container); err != nil {
		return err
	}

	return nil
}

// delete
func (d *Driver) delete(trans Transaction) error {

	mutex := d.getOrCreateMutex(trans.Collection)
	mutex.Lock()

	dir := d.dir + "/" + trans.Collection

	err := os.Remove(dir + "/" + trans.ResourceID)
	if err != nil {
		return err
	}

	mutex.Unlock()

	return nil
}

// helpers

// getOrCreateMutex
func (d *Driver) getOrCreateMutex(collection string) sync.Mutex {

	c, ok := d.mutexes[collection]

	// if the mutex doesn't exist make it
	if !ok {
		d.mutexes[collection] = sync.Mutex{}
		return d.mutexes[collection]
	}

	return c
}

// mkDir
func mkDir(d string) error {

	//
	dir, _ := os.Stat(d)

	if dir == nil {
		err := os.MkdirAll(d, 0755)
		if err != nil {
			return err
		}
	}

	return nil
}
