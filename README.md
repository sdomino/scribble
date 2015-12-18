scribble
--------

A tiny JSON database in Golang


### Installation

Install using `go get github.com/nanobox-io/golang-scribble`.


### Usage

```go
// a new scribble driver, providing the directory where it will be writing to,
// and a qualified logger if desired
db, err := scribble.New(dir, nil)
if err != nil {
  fmt.Println("Error", err)
}

// Write a fish to the database
fish := Fish{}
if err := db.Write("fish", "onefish", fish); err != nil {
  fmt.Println("Error", err)
}

// Read a fish from the database (passing fish by reference)
fish := []Fish{}
if err := db.Read("fish", "onefish", &fish); err != nil {
  fmt.Println("Error", err)
}

// Read all fish from the database, unmarshaling the response.
records, err := db.ReadAll("fish")
if err != nil {
  fmt.Println("Error", err)
}

fish := []Fish{}
if err := json.Unmarshal([]byte(records), &fish); err != nil {
  fmt.Println("Error", err)
}

// Delete a fish from the database
if err := db.Delete("fish", "onefish"); err != nil {
  fmt.Println("Error", err)
}

// Delete all fish from the database
if err := db.Delete("fish", ""); err != nil {
  fmt.Println("Error", err)
}
```

## Documentation
Complete documentation is available on [godoc](http://godoc.org/github.com/nanobox-io/golang-scribble).


## Todo/Doing
- Support for windows
- Better support for concurrency
- Better support for sub collections
- More methods to allow different types of reads/writes
- More tests (you can never have enough!)


## Contributing
1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
