// Copyright (c) 2015 Pagoda Box Inc
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v.
// 2.0. If a copy of the MPL was not distributed with this file, You can obtain one
// at http://mozilla.org/MPL/2.0/.
//

// scribble is a tiny JSON flat file store. It uses transactions that tell it what
// actions to perform, where it is to store data, and what it is going to write
// that data from, or read the data into. It creates a very simple database
// structure under a specified directory
package scribble

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"

	"github.com/nanobox-io/golang-hatchet"
)

const Version = "0.1.0"

type (

	// a Driver is what is used to interact with the scribble database. It runs
	// transactions, and provides log output
	Driver struct {
		mutexes map[string]sync.Mutex
		dir     string         // the directory where scribble will create the database
		log     hatchet.Logger // the logger scirbble will log to
	}

	// a Transactions is what is used by a Driver to complete database operations
	Transaction struct {
		Action     int         // the action for scribble to preform
		Collection string      // the forlder for scribble to read/write to/from
		ResourceID string      // the unique ID of the record
		Container  interface{} // what scribble will marshal from or unmarshal into
	}
)

//
const (
	WRITE = iota
	READ
	READALL
	DELETE
)

// New creates a new scribble database at the desired directory location, and
// returns a *Driver to then use for interacting with the database
func New(dir string, logger hatchet.Logger) (*Driver, error) {
	fmt.Printf("Creating database directory at '%v'...\n", dir)

	//
	if logger == nil {
		logger = hatchet.DevNullLogger{}
	}

	//
	scribble := &Driver{
		dir:     dir,
		mutexes: make(map[string]sync.Mutex),
		log:     logger,
	}

	// create database
	if err := mkDir(scribble.dir); err != nil {
		return nil, err
	}

	//
	return scribble, nil
}

// Transact determins the type of transactions to be run, and calls the appropriate
// method to complete the operation
func (d *Driver) Transact(trans Transaction) (err error) {

	// determin transaction to be run
	switch trans.Action {
	case WRITE:
		return d.write(trans)
	case READ:
		return d.read(trans)
	case READALL:
		return d.readAll(trans)
	case DELETE:
		return d.delete(trans)
	default:
		return errors.New(fmt.Sprintf("Unsupported action %+v", trans.Action))
	}

	return
}

// write locks the database and then proceeds to write the data represented by a
// transaction.Container. It will create a direcotry that represents the collection
// to wich the record belongs (if it doesn't already exist), and write a file
// (named by he transaction.ResourceID) to that directory
func (d *Driver) write(trans Transaction) error {

	mutex := d.getOrCreateMutex(trans.Collection)
	mutex.Lock()
	defer mutex.Unlock()

	//
	dir := d.dir + "/" + trans.Collection

	//
	b, err := json.MarshalIndent(trans.Container, "", "\t")
	if err != nil {
		return err
	}

	// create collection directory
	if err := mkDir(dir); err != nil {
		return err
	}

	finalPath := dir + "/" + trans.ResourceID + ".json"
	tmpPath := finalPath + "~"

	// write marshaled data to a file, named by the resourceID
	if err := ioutil.WriteFile(tmpPath, b, 0644); err != nil {
		return err
	}

	// move final file into place
	return os.Rename(tmpPath, finalPath)
}

// read does the opposite operation as write. Reading a record from the database
// (named by the transaction.resourceID, found in the transaction.Collection), and
// unmarshaling the data into the transaction.Container
func (d *Driver) read(trans Transaction) error {

	dir := d.dir + "/" + trans.Collection

	// read record from database
	b, err := ioutil.ReadFile(dir + "/" + trans.ResourceID + ".json")
	if err != nil {
		return err
	}

	// unmarshal data into the transaction.Container
	return json.Unmarshal(b, trans.Container)
}

// readAll does the same operation as read, reading all the records in the specified
// transaction.Collection
func (d *Driver) readAll(trans Transaction) error {

	dir := d.dir + "/" + trans.Collection

	// read all the files in the transaction.Collection
	files, err := ioutil.ReadDir(dir)

	if err != nil {
		// an error here just means an empty collection so do nothing
	}

	// the files read from the database
	var f []string

	// iterate over each of the files, attempting to read the file. If successful
	// append the files to the collection of read files
	for _, file := range files {
		b, err := ioutil.ReadFile(dir + "/" + file.Name())
		if err != nil {
			return err
		}

		// append read file
		f = append(f, string(b))
	}

	// unmarhsal the read files as a comma delimeted byte array
	return json.Unmarshal([]byte("["+strings.Join(f, ",")+"]"), trans.Container)
}

// delete locks that database and then proceeds to remove the record (specified by
// transaction.ResourceID) from the collection
func (d *Driver) delete(trans Transaction) error {

	mutex := d.getOrCreateMutex(trans.Collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := d.dir + "/" + trans.Collection

	// remove record from database
	return os.Remove(dir + "/" + trans.ResourceID + ".json")
}

// helpers

// getOrCreateMutex creates a new collection specific mutex any time a collection
// is being modfied to avoid unsafe operations
func (d *Driver) getOrCreateMutex(collection string) sync.Mutex {

	c, ok := d.mutexes[collection]

	// if the mutex doesn't exist make it
	if !ok {
		c = sync.Mutex{}
		d.mutexes[collection] = c
	}

	return c
}

// mkDir is a simple wrapper that attempts to make a directory at a specified
// location
func mkDir(d string) (err error) {

	//
	dir, _ := os.Stat(d)

	switch {
	case dir == nil:
		err = os.MkdirAll(d, 0755)
	case !dir.IsDir():
		err = os.ErrInvalid
	}

	return
}
