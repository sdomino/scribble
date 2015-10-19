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
	"path/filepath"
	"strings"
	"sync"

	"github.com/nanobox-io/golang-hatchet"
)

const Version = "1.0.0"

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

	//
	dir = filepath.Clean(dir)

	//
	if logger == nil {
		logger = hatchet.DevNullLogger{}
	}

	logger.Info("Creating database directory at '%v'...\n", dir)

	//
	d := &Driver{
		dir:     dir,
		mutexes: make(map[string]sync.Mutex),
		log:     logger,
	}

	// create database
	if err := mkDir(d.dir); err != nil {
		return nil, err
	}

	//
	return d, nil
}

// Read a record from the database
func (d *Driver) Read(path string, v interface{}) error {

	//
	dir := filepath.Join(d.dir, path)

	//
	switch fi, err := stat(dir); {

	// if fi is nil or error is not nil return
	case fi == nil, err != nil:
		return fmt.Errorf("Unable to find file or directory named %v\n", path)

	// if the path is a directory, attempt to read all entries into v
	case fi.Mode().IsDir():

		// read all the files in the transaction.Collection; an error here just means
		// the collection is either empty or doesn't exist
		files, _ := ioutil.ReadDir(dir)

		// the files read from the database
		var f []string

		// iterate over each of the files, attempting to read the file. If successful
		// append the files to the collection of read files
		for _, file := range files {
			b, err := ioutil.ReadFile(filepath.Join(dir, file.Name()))
			if err != nil {
				return err
			}

			// append read file
			f = append(f, string(b))
		}

		// unmarhsal the read files as a comma delimeted byte array
		return json.Unmarshal([]byte("["+strings.Join(f, ",")+"]"), v)

		// if the path is a file, attempt to read the single file
	case fi.Mode().IsRegular():

		// read record from database
		b, err := ioutil.ReadFile(dir + ".json")
		if err != nil {
			return err
		}

		// unmarshal data
		return json.Unmarshal(b, &v)
	}

	return nil
}

// Write locks the database and attempts to write the record to the database under
// the [collection] specified with the [resource] name given
func (d *Driver) Write(collection, resource string, v interface{}) error {

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	//
	dir := filepath.Join(d.dir, collection)

	//
	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}

	// create collection directory
	if err := mkDir(dir); err != nil {
		return err
	}

	finalPath := filepath.Join(dir, resource+".json")
	tmpPath := finalPath + "~"

	// write marshaled data to the temp file
	if err := ioutil.WriteFile(tmpPath, b, 0644); err != nil {
		return err
	}

	// move final file into place
	return os.Rename(tmpPath, finalPath)
}

// Delete locks that database and then attempts to remove the collection/resource
// specified by [path]
func (d *Driver) Delete(path string) error {

	//
	mutex := d.getOrCreateMutex(path)
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
