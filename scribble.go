package scribble

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/nanobox-core/hatchet"
)

//
const Version = "0.0.1"

//
type (

	// Driver
	Driver struct {
		channels map[string]chan int
		dir      string
		log      *hatchet.Logger
	}

	// Transaction represents
	Transaction struct {
		Action     string
		Collection string
		Resource   string
		Container  interface{}
	}
)

// New
func New(dir string, logger hatchet.Logger) (*Driver, error) {
	fmt.Printf("Creating database directory at '%v'...\n", dir)

	scribble := &Driver{}
	scribble.dir = dir
	scribble.channels = make(map[string]chan int)

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
	done := d.getOrCreateChan(trans.Collection)
	fail := make(chan error)

	//
	switch trans.Action {
	case "write":
		go d.write(trans, done, fail)
	case "read":
		go d.read(trans, done, fail)
	case "readall":
		go d.readAll(trans, done, fail)
	case "delete":
		go d.delete(trans, done, fail)
	default:
		fmt.Println("Unsupported action ", trans.Action)
	}

	// wait until we're done, or error
	select {
	case <-done:
		return nil
	case err := <-fail:
		return err
	}
}

// private

// write
func (d *Driver) write(trans Transaction, done chan<- int, fail chan<- error) {

	//
	dir := d.dir + "/" + trans.Collection

	//
	if err := mkDir(dir); err != nil {
		fail <- err
	}

	//
	file, err := os.Create(dir + "/" + trans.Resource)
	if err != nil {
		fail <- err
	}

	defer file.Close()

	//
	b, err := json.MarshalIndent(trans.Container, "", "\t")
	if err != nil {
		fail <- err
	}

	_, err = file.WriteString(string(b))
	if err != nil {
		fail <- err
	}

	// release...
	done <- 0
}

// read
func (d *Driver) read(trans Transaction, done chan<- int, fail chan<- error) interface{} {

	dir := d.dir + "/" + trans.Collection

	b, err := ioutil.ReadFile(dir + "/" + trans.Resource)
	if err != nil {
		fmt.Printf("Unable to read file %v/%v: %v", trans.Collection, trans.Resource, err)
		os.Exit(1)
	}

	if err := json.Unmarshal(b, trans.Container); err != nil {
		fail <- err
	}

	// release...
	done <- 0

	return trans.Container
}

// readAll
func (d *Driver) readAll(trans Transaction, done chan<- int, fail chan<- error) {

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
			fail <- err
		}

		f = append(f, string(b))
	}

	//
	if err := json.Unmarshal([]byte("["+strings.Join(f, ",")+"]"), trans.Container); err != nil {
		fail <- err
	}

	// release...
	done <- 0
}

// delete
func (d *Driver) delete(trans Transaction, done chan<- int, fail chan<- error) {

	dir := d.dir + "/" + trans.Collection

	err := os.Remove(dir + "/" + trans.Resource)
	if err != nil {
		fail <- err
	}

	// release...
	done <- 0
}

// helpers

// getChan
func (d *Driver) getOrCreateChan(channel string) chan int {

	c, ok := d.channels[channel]

	// if the chan doesn't exist make it
	if !ok {
		d.channels[channel] = make(chan int)
		return d.channels[channel]
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
