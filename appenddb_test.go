package appenddb

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func Test1(t *testing.T) {
	td, err := ioutil.TempDir("", "")
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(td)
	path := filepath.Join(td, "l1")
	func() {
		ad, err := NewV1(path)
		if err != nil {
			t.Error(err)
		}
		defer ad.Close()
		l, err := ad.Len()
		if err != nil {
			t.Error(err)
		}
		if l != 0 {
			t.Fatalf("ad.Len() == %+v != 0", l)
		}
		v, err := ad.Get(-1)
		if err == nil {
			t.Fatalf("No error was raised: %+v", v)
		}
		v, err = ad.Get(0)
		if err == nil {
			t.Fatalf("No error was raised: %+v", v)
		}
		v, err = ad.Get(1)
		if err == nil {
			t.Fatalf("No error was raised: %+v", v)
		}
		err = ad.Append("どうだろう？\n")
		if err != nil {
			t.Fatalf("Unexpected: %+v", err)
		}
		v, err = ad.Get(-1)
		if err == nil {
			t.Fatalf("No error was raised: %+v", v)
		}
		v, err = ad.Get(1)
		if err == nil {
			t.Fatalf("No error was raised: %+v", v)
		}
		v, err = ad.Get(0)
		if err != nil {
			t.Fatalf("Unexpected: %+v", err)
		}
		if v != "どうだろう？\n" {
			t.Fatalf("%+v != %+v", v, "どうだろう？\n")
		}
		err = ad.Append("もう1つ\n")
		if err != nil {
			t.Fatalf("Unexpected: %+v", err)
		}
		v, err = ad.Get(0)
		if err != nil {
			t.Fatalf("Unexpected: %+v", err)
		}
		if v != "どうだろう？\n" {
			t.Fatalf("%+v != %+v", v, "どうだろう？\n")
		}
		v, err = ad.Get(1)
		if err != nil {
			t.Fatalf("Unexpected: %+v", err)
		}
		if v != "もう1つ\n" {
			t.Fatalf("%+v != %+v", v, "もう1つ\n")
		}
		v, err = ad.Get(2)
		if err == nil {
			t.Fatalf("No error was raised: %+v", v)
		}
	}()
	func() {
		ad, err := NewV1(path)
		if err != nil {
			t.Error(err)
		}
		defer ad.Close()
		l, err := ad.Len()
		if err != nil {
			t.Error(err)
		}
		if l != 2 {
			t.Fatalf("ad.Len() == %+v != 2", l)
		}
		v, err := ad.Get(1)
		if err != nil {
			t.Fatalf("Unexpected: %+v", err)
		}
		if v != "もう1つ\n" {
			t.Fatalf("%+v != %+v", v, "もう1つ\n")
		}
		v, err = ad.Get(0)
		if err != nil {
			t.Fatalf("Unexpected: %+v", err)
		}
		if v != "どうだろう？\n" {
			t.Fatalf("%+v != %+v", v, "どうだろう？\n")
		}
		err = ad.Append("OK??\n")
		if err != nil {
			t.Fatalf("Unexpected: %+v", err)
		}
		v, err = ad.Get(2)
		if err != nil {
			t.Fatalf("Unexpected: %+v", err)
		}
		if v != "OK??\n" {
			t.Fatalf("%+v != %+v", v, "OK??\n")
		}
	}()
}
