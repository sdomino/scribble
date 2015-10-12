package scribble

import (
	"fmt"
	"sync"
	"testing"
)

type logger struct {
	t *testing.T
}

func (l logger) Fatal(f string, a ...interface{}) { l.t.Fatalf(f, a...) }
func (l logger) Error(f string, a ...interface{}) { l.t.Fatalf(f, a...) }
func (l logger) Warn(f string, a ...interface{})  { l.t.Fatalf(f, a...) }
func (l logger) Info(f string, a ...interface{})  {}
func (l logger) Debug(f string, a ...interface{}) {}
func (l logger) Trace(f string, a ...interface{}) {}

func TestBasic(t *testing.T) {
	var d *Driver
	var err error

	if d, err = New("./test-dir", logger{t}); err != nil {
		t.Fatal(err)
	}

	if err = d.Write("/fish", "big", "small"); err != nil {
		t.Fatal(err)
	}

	var ans string

	if err = d.Read("/fish/big", &ans); err != nil {
		t.Fatal(err)
	}

	if ans != "small" {
		t.Fatal("Expected 'small' but read back ", ans)
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			if err1 := d.Write("/fish", fmt.Sprintf("num%v", i), fmt.Sprintf("%v", i)); err1 != nil {
				t.Fatal(err1)
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 10; i < 20; i++ {
			if err1 := d.Write("/fish", fmt.Sprintf("num%v", i), fmt.Sprintf("%v", i)); err1 != nil {
				t.Fatal(err1)
				return
			}
		}
	}()

	wg.Wait()

	var fishes []string

	if err := d.Read("/fish", &fishes); err != nil {
		t.Fatal(err)
	}

	if len(fishes) != 21 {
		t.Fatalf("Expected 21 entries but found %v", len(fishes))
	}

}
