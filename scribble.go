// Copyright (c) 2015 Pagoda Box Inc
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v.
// 2.0. If a copy of the MPL was not distributed with this file, You can obtain one
// at http://mozilla.org/MPL/2.0/.
//

// scribble is a tiny JSON flat file store
package scribble

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"

	"github.com/nanobox-io/golang-hatchet"
)

const Version = "0.5.0"

type (

	// a Driver is what is used to interact with the scribble database. It runs
	// transactions, and provides log output
	Driver struct {
		mutexes map[string]sync.Mutex
		dir     string         // the directory where scribble will create the database
		log     hatchet.Logger // the logger scribble will log to
	}
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

// Write locks the database and attempts to write the record to the database under
// the [collection] specified with the [resource] name given
//
// Example:
//
// // write [fish] to database
// Write("/fish/onefish", fish{name:"onefish"})
func (d *Driver) Write(collection, resource string, v interface{}) error {

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	//
	dir := d.dir + "/" + collection

	//
	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}

	// create collection directory
	if err := mkDir(dir); err != nil {
		return err
	}

	finalPath := dir + "/" + resource + ".json"
	tmpPath := finalPath + "~"

	// write marshaled data to the temp file
	if err := ioutil.WriteFile(tmpPath, b, 0644); err != nil {
		return err
	}

	// move final file into place
	return os.Rename(tmpPath, finalPath)
}

// Read a record from the database
//
// Example:
//
// // read a single fish
// Read("/fish/twofish", &fish)
//
// // read all fish
// Read("/fish", &fish)
func (d *Driver) Read(path string, v interface{}) error {

	dir := d.dir + "/" + path

	//
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}

	switch {

	// if the path is a directory, attempt to read all entries into v
	case fi.Mode().IsDir():

		// read all the files in the transaction.Collection
		files, err := ioutil.ReadDir(dir)
		if err != nil {
			// an error here just means the collection is either empty or doesn't exist
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
		return json.Unmarshal([]byte("["+strings.Join(f, ",")+"]"), v)

		// if the path is a file, attempt to read the single file
	case !fi.Mode().IsDir():
		// read record from database
		b, err := ioutil.ReadFile(dir + ".json")
		if err != nil {
			return err
		}

		// unmarshal data into the transaction.Container
		return json.Unmarshal(b, &v)
	}

	return nil
}

// Delete locks that database and then attempts to remove the collection/resource
// specified by [path]
//
// Example:
//
// // delete the fish 'redfish.json'
// Delete("/fish/redfish")
//
// // delete the fish collection
// Delete("/fish")
func (d *Driver) Delete(path string) error {

	mutex := d.getOrCreateMutex(path)
	mutex.Lock()
	defer mutex.Unlock()

	// stat the file to determine if it is a file or dir
	fi, err := os.Stat(path)
	if err != nil {
		return err
	}

	switch {
	// remove the collection from database
	case fi.Mode().IsDir():
		return os.Remove(d.dir + "/" + path)

		// remove the record from database
	default:
		return os.Remove(d.dir + "/" + path + ".json")
	}
}

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
