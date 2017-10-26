package main

import (
	"encoding/json"
	"fmt"

	scribble "github.com/nanobox-io/golang-scribble"
)

// a fish
type Fish struct{ Name string }

func main() {

	dir := "./"

	db, err := scribble.New(dir, nil)
	if err != nil {
		fmt.Println("Error", err)
	}

	// Write a fish to the database
	for _, name := range []string{"onefish", "twofish", "redfish", "bluefish"} {
		db.Write("fish", name, Fish{Name: name})
	}

	// Read a fish from the database (passing fish by reference)
	onefish := Fish{}
	if err := db.Read("fish", "onefish", &onefish); err != nil {
		fmt.Println("Error", err)
	}

	// Read all fish from the database, unmarshaling the response.
	records, err := db.ReadAll("fish")
	if err != nil {
		fmt.Println("Error", err)
	}

	fishies := []Fish{}
	for _, f := range records {
		fishFound := Fish{}
		if err := json.Unmarshal([]byte(f), &fishFound); err != nil {
			fmt.Println("Error", err)
		}
		fishies = append(fishies, fishFound)
	}

	// // Delete a fish from the database
	// if err := db.Delete("fish", "onefish"); err != nil {
	// 	fmt.Println("Error", err)
	// }
	//
	// // Delete all fish from the database
	// if err := db.Delete("fish", ""); err != nil {
	// 	fmt.Println("Error", err)
	// }

}
