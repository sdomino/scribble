package scribble

import (
	"os"
	"testing"
)

//
type Fish struct {
	Type string `json:"type"`
}

//
var (
	db         *Driver
	database   = "./deep/school"
	collection = "fish"
	onefish    = Fish{}
	twofish    = Fish{}
	redfish    = Fish{Type: "red"}
	bluefish   = Fish{Type: "blue"}
)

//
func TestMain(m *testing.M) {

	// remove any thing for a potentially failed previous test
	os.RemoveAll("./deep")

	// run
	code := m.Run()

	// cleanup
	os.RemoveAll("./deep")

	// exit
	os.Exit(code)
}

// Tests creating a new database, and using an existing database
func TestNew(t *testing.T) {

	// database should not exist
	if _, err := os.Stat(database); err == nil {
		t.Error("Expected nothing, got database")
	}

	// create a new database
	createDB()

	// database should exist
	if _, err := os.Stat(database); err != nil {
		t.Error("Expected database, got nothing")
	}

	// should use existing database
	createDB()

	// database should exist
	if _, err := os.Stat(database); err != nil {
		t.Error("Expected database, got nothing")
	}
}

//
func TestWriteAndRead(t *testing.T) {

	createDB()

	// add fish to database
	if err := db.Write(collection, "redfish", redfish); err != nil {
		t.Error("Create fish failed: ", err.Error())
	}

	// read fish from database
	if err := db.Read(collection, "redfish", &onefish); err != nil {
		t.Error("Failed to read: ", err.Error())
	}

	//
	if onefish.Type != "red" {
		t.Error("Expected red fish, got: ", onefish.Type)
	}

	destroySchool()
}

//
func TestReadall(t *testing.T) {

	createDB()
	createSchool()

	fish, err := db.ReadAll(collection)
	if err != nil {
		t.Error("Failed to read: ", err.Error())
	}

	if len(fish) <= 0 {
		t.Error("Expected some fish, have none")
	}

	destroySchool()
}

//
func TestWriteAndReadEmpty(t *testing.T) {

	createDB()

	// create a fish with no home
	if err := db.Write("", "redfish", redfish); err == nil {
		t.Error("Allowed write of empty resource", err.Error())
	}

	// create a home with no fish
	if err := db.Write(collection, "", redfish); err == nil {
		t.Error("Allowed write of empty resource", err.Error())
	}

	// no place to read
	if err := db.Read("", "redfish", onefish); err == nil {
		t.Error("Allowed read of empty resource", err.Error())
	}

	destroySchool()
}

//
func TestDelete(t *testing.T) {

	createDB()

	// add fish to database
	if err := db.Write(collection, "redfish", redfish); err != nil {
		t.Error("Create fish failed: ", err.Error())
	}

	// delete the fish
	if err := db.Delete(collection, "redfish"); err != nil {
		t.Error("Failed to delete: ", err.Error())
	}

	// read fish from database
	if err := db.Read(collection, "redfish", &onefish); err == nil {
		t.Error("Expected nothing, got fish")
	}

	destroySchool()
}

//
func TestDeleteall(t *testing.T) {

	createDB()
	createSchool()

	if err := db.Delete(collection, ""); err != nil {
		t.Error("Failed to delete: ", err.Error())
	}

	if _, err := os.Stat(collection); err == nil {
		t.Error("Expected nothing, have fish")
	}

	destroySchool()
}

//

// create a new scribble database
func createDB() error {
	var err error
	if db, err = New(database, nil); err != nil {
		return err
	}

	return nil
}

// create a fish
func createFish(fish Fish) error {
	return db.Write(collection, fish.Type, fish)
}

// create many fish
func createSchool() error {
	for _, f := range []Fish{{Type: "red"}, {Type: "blue"}} {
		if err := db.Write(collection, f.Type, f); err != nil {
			return err
		}
	}

	return nil
}

// destroy a fish
func destroyFish(name string) error {
	return db.Delete(collection, name)
}

// destroy all fish
func destroySchool() error {
	return db.Delete(collection, "")
}
