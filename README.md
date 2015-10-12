scribble
--------

`golang-scribble` (Golang package: `scribble`) is a tiny JSON flat file store


### Installation

Install using `go get github.com/nanobox-io/golang-scribble`.


### Usage

Create a 'transaction' for scribble to transact.

`t := scribble.Transaction{Action: "read", Collection: "records", ResourceID: "<UniqueID>", Container: &v}`

+ Action - the action for scribble to perform
  + write - write to the scribble db
  + read - read from the scribble db
  + readall - read from the scribble db (all files in a collection)
  + delete - remove a record from the scribble db
+ Collection - the folder scribble will create to store grouped records
+ ResourceID - the unique ID of the resource being stored (bson, uuid, etc.)
+ Container - the Struct that contains the data scribble will marshal into the store, or what it will unmarshal into from the store


#### Full Example
```go
// a new scribble driver, providing the directory where it will be writing to, and a qualified logger to which it can send any output.
database, err := scribble.New(dir, logger)
if err != nil {
  fmt.Println("Error", err)
}

// this is what scribble will either marshal from when writing, or unmarshal into when reading
record := Record{}

// create a new transaction for scribble to run
t := scribble.Transaction{Action: "read", Collection: "records", ResourceID: "<UniqueID>", Container: &record}

// have scribble attempt to run the transaction
if err := database.Transact(t); err != nil {
  fmt.Println("Error", err)
}
```

For an example of a qualified logger see [here](http://godoc.org/github.com/nanobox-io/golang-hatchet).


## Documentation
Complete documentation is available on [godoc](http://godoc.org/github.com/nanobox-io/golang-scribble).


## Todo/Doing
- Tests!


## Contributing
1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
