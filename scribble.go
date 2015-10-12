// Copyright (c) 2015 Pagoda Box Inc
//
// This Source Code Form is subject to the terms of the Mozilla Public License, v.
// 2.0. If a copy of the MPL was not distributed with this file, You can obtain one
// at http://mozilla.org/MPL/2.0/.
//

// scribble is a tiny JSON flat file store
package scribble

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/nanobox-io/golang-hatchet"
)

const Version = "0.5.0"

type (

	// a Driver is what is used to interact with the scribble database. It runs
	// transactions, and provides log output
	Driver struct {
		maplock sync.RWMutex
		mutexes map[string]sync.Mutex
		dir     string         // the directory where scribble will create the database
		log     hatchet.Logger // the logger scribble will log to
	}
)

// New creates a new scribble database at the desired directory location, and
// returns a *Driver to then use for interacting with the database
func New(dir string, logger hatchet.Logger) (*Driver, error) {
	dir = filepath.Clean(dir)

	fmt.Printf("Creating database directory at '%v'...\n", dir)

	//
	if logger == nil {
		logger = hatchet.DevNullLogger{}
	}

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

	if _, err := os.Stat(finalPath); err == nil {
		if _, err = os.Stat(finalPath + ".bak"); err == nil {
			if err = os.Remove(finalPath + ".bak"); err != nil {
				return err
			}
		}
		if err = os.Rename(finalPath, finalPath+".bak"); err != nil {
			return err
		}
	}

	// move final file into place
	return os.Rename(tmpPath, finalPath)
}

// Read a record from the database
func (d *Driver) Read(path string, v interface{}) error {

	var err error
	var fi os.FileInfo

	dir := filepath.Join(d.dir, path)

	//
	fi, err = os.Stat(dir)

	if err == nil {
		if !fi.Mode().IsDir() {
			return fmt.Errorf("Expected path %v to be a folder", path)
		}

		var files []os.FileInfo

		// read all the files in the transaction.Collection
		files, err = ioutil.ReadDir(dir)
		if err != nil {
			// an error here just means the collection is either empty or doesn't exist
		}

		buf := bytes.Buffer{}

		buf.WriteString("[")

		// the files read from the database
		if len(files) > 0 {

			// iterate over each of the files, attempting to read the file. If successful
			// append the files to the collection of read files
			for _, file := range files {
				if !strings.HasSuffix(file.Name(), ".json") {
					continue
				}

				b, err := ioutil.ReadFile(filepath.Join(dir, file.Name()))
				if err != nil {
					return err
				}

				// append read file
				buf.Write(b)
				buf.WriteString(",")
			}
			buf.Truncate(buf.Len() - len(","))
		}

		buf.WriteString("]")

		// unmarhsal the read files as a comma delimeted byte array
		return json.Unmarshal(buf.Bytes(), v)
	}

	fi, err = os.Stat(dir + ".json")
	if err != nil {
		return err
	}

	var b []byte
	b, err = ioutil.ReadFile(dir + ".json")
	if err != nil {
		return err
	}

	// unmarshal data into the transaction.Container
	return json.Unmarshal(b, &v)

}

// Delete locks that database and then attempts to remove the collection/resource
// specified by [path]
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
		return os.Remove(filepath.Join(d.dir, path))

		// remove the record from database
	default:
		return os.Remove(filepath.Join(d.dir, path, ".json"))
	}
}

// getOrCreateMutex creates a new collection specific mutex any time a collection
// is being modfied to avoid unsafe operations
func (d *Driver) getOrCreateMutex(collection string) sync.Mutex {

	d.maplock.RLock()

	c, ok := d.mutexes[collection]

	d.maplock.RUnlock()
	// if the mutex doesn't exist make it
	if !ok {

		d.maplock.Lock()
		c = sync.Mutex{}
		d.mutexes[collection] = c
		d.maplock.Unlock()
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
