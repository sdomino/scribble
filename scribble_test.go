package scribble

import (
	"os"
	"testing"
)

//
type Friend struct {
	Name string `json:"name"`
}

//
var (
	db          *Driver
	testRoot    = "./test_db"
	friendsPath = "/friends"
	friend0     = Friend{}
	friend1     = Friend{Name: "wocket"}
	friend2     = Friend{Name: "wasket"}
)

//
func init() {
	startup()
}

//
func startup() {
	db, _ = New(testRoot, nil)
}

//
func teardown() {
	os.RemoveAll(testRoot)
}

//
func createFriend(t *testing.T) {
	if err := db.Write(friendsPath, "friend1", friend1); err != nil {
		t.Error("Failed to write", err)
	}
}

//
func createFriends(t *testing.T) {
	if err := db.Write(friendsPath, "friend1", friend1); err != nil {
		t.Error("Failed to write", err)
	}

	if err := db.Write(friendsPath, "friend2", friend2); err != nil {
		t.Error("Failed to write", err)
	}
}

//
func TestNew(t *testing.T) {
	if _, err := os.Stat(testRoot); os.IsNotExist(err) {
		t.Error("Expected file, got none", err)
	}

	teardown()
}

//
func TestWrite(t *testing.T) {
	createFriend(t)
	teardown()
}

//
func TestRead(t *testing.T) {
	createFriend(t)

	if err := db.Read(friendsPath+"/friend1", &friend0); err != nil {
		t.Error("Failed to read", err)
	}

	if friend0.Name == "" {
		t.Error("Expected friend, have none")
	}

	teardown()
}

//
func TestReadEmpty(t *testing.T) {

	if err := db.Read(friendsPath+"/friend1", &friend0); err == nil {
		t.Error("Expected nothing, found friend")
	}

	teardown()
}

//
func TestReadall(t *testing.T) {
	createFriends(t)

	friends := []Friend{}
	if err := db.Read(friendsPath, &friends); err != nil {
		t.Error("Failed to read", err)
	}

	if len(friends) <= 0 {
		t.Error("Expected friends, have none")
	}

	teardown()
}

//
func TestReadallEmpty(t *testing.T) {

	friends := []Friend{}
	if err := db.Read(friendsPath, &friends); err == nil {
		t.Error("Expected nothing, found friends")
	}

	teardown()
}

//
func TestDelete(t *testing.T) {
	createFriend(t)

	if err := db.Delete(friendsPath + "/friend1"); err != nil {
		t.Error("Failed to delete", err)
	}

	if fi, err := os.Stat(friendsPath + "/friend1"); fi != nil {
		t.Error("Expected nothing, have friends", err)
	}

	teardown()
}

//
func TestDeleteall(t *testing.T) {
	createFriends(t)

	if err := db.Delete(friendsPath); err != nil {
		t.Error("Failed to delete ", err)
	}

	if fi, err := os.Stat(friendsPath); fi != nil {
		t.Error("Expected nothing, have friends", err)
	}

	teardown()
}
