package scribble

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/nanobox-core/utils"
)

const (
	DefaultDir = "./tmp/db"
	Version    = "0.0.1"
)

//
type (

	// Driver represents
	Driver struct {
		channels map[string]chan int
		dir      string
	}

	// Transaction represents
	Transaction struct {
		Action     string
		Collection string
		Resource   string
		Container  interface{}
	}
)

//
var (
	debugging bool
)

// Init
func (d *Driver) Init(opts map[string]string) int {
	fmt.Printf("Creating database directory at '%v'...\n", opts["db_dir"])

	debugging = (opts["debugging"] == "true")

	d.dir = opts["db_dir"]

	//
	d.channels = make(map[string]chan int)

	// make a ping channel
	ping := make(chan int)
	d.channels["ping"] = ping

	//
	if err := mkDir(d.dir); err != nil {
		fmt.Printf("Unable to create dir '%v': %v", d.dir, err)
		return 1
	}

	//
	return 0
}

// Transact
func (d *Driver) Transact(trans Transaction) {

	//
	done := d.getOrCreateChan(trans.Collection)

	//
	switch trans.Action {
	case "write":
		go d.write(trans, done)
	case "read":
		go d.read(trans, done)
	case "readall":
		go d.readAll(trans, done)
	case "delete":
		go d.delete(trans, done)
	default:
		fmt.Println("Unsupported action ", trans.Action)
	}

	// wait...
	<-done
}

// private

// write
func (d *Driver) write(trans Transaction, done chan<- int) {

	//
	dir := d.dir + "/" + trans.Collection

	//
	if err := mkDir(dir); err != nil {
		fmt.Println("Unable to create dir '%v': %v", dir, err)
		os.Exit(1)
	}

	//
	file, err := os.Create(dir + "/" + trans.Resource)
	if err != nil {
		fmt.Printf("Unable to create file %v/%v: %v", trans.Collection, trans.Resource, err)
		os.Exit(1)
	}

	defer file.Close()

	//
	b := utils.ToJSONIndent(trans.Container)

	_, err = file.WriteString(string(b))
	if err != nil {
		fmt.Printf("Unable to write to file %v: %v", trans.Resource, err)
		os.Exit(1)
	}

	// release...
	done <- 0
}

// read
func (d *Driver) read(trans Transaction, done chan<- int) interface{} {

	dir := d.dir + "/" + trans.Collection

	b, err := ioutil.ReadFile(dir + "/" + trans.Resource)
	if err != nil {
		fmt.Printf("Unable to read file %v/%v: %v", trans.Collection, trans.Resource, err)
		os.Exit(1)
	}

	if err := utils.FromJSON(b, trans.Container); err != nil {
		panic(err)
	}

	// release...
	done <- 0

	return trans.Container
}

// readAll
func (d *Driver) readAll(trans Transaction, done chan<- int) {

	dir := d.dir + "/" + trans.Collection

	//
	files, err := ioutil.ReadDir(dir)

	// if there is an error here it just means there are no evars so dont do anything
	if err != nil {
	}

	var f []string

	for _, file := range files {
		b, err := ioutil.ReadFile(dir + "/" + file.Name())
		if err != nil {
			panic(err)
		}

		f = append(f, string(b))
	}

	//
	if err := utils.FromJSON([]byte("["+strings.Join(f, ",")+"]"), trans.Container); err != nil {
		panic(err)
	}

	// release...
	done <- 0
}

// delete
func (d *Driver) delete(trans Transaction, done chan<- int) {

	dir := d.dir + "/" + trans.Collection

	err := os.Remove(dir + "/" + trans.Resource)
	if err != nil {
		fmt.Printf("Unable to delete file %v/%v: %v", trans.Collection, trans.Resource, err)
		os.Exit(1)
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
