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
	database   = "./school"
	collection = "fish"
	onefish    = Fish{}
	twofish    = Fish{}
	redfish    = Fish{Type: "red"}
	bluefish   = Fish{Type: "blue"}
)

//
func TestMain(m *testing.M) {

	var err error

	// create a new scribble
	if db, err = New(database, nil); err != nil {
		panic(err)
	}

	// run
	code := m.Run()

	// cleanup
	os.RemoveAll(database)

	// exit
	os.Exit(code)
}

//
func TestNew(t *testing.T) {
	if _, err := os.Stat(database); err != nil {
		t.Error("Expected file, got none")
	}
}

//
func TestWriteAndRead(t *testing.T) {

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
func createFish(fish Fish) error {
	return db.Write(collection, fish.Type, fish)
}

//
func createSchool() error {
	for _, f := range []Fish{Fish{Type: "red"}, Fish{Type: "blue"}} {
		if err := db.Write(collection, f.Type, f); err != nil {
			return err
		}
	}

	return nil
}

//
func destroyFish(name string) error {
	return db.Delete(collection, name)
}

//
func destroySchool() error {
	return db.Delete(collection, "")
}
