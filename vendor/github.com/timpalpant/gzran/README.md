## gzran
[![GoDoc](https://godoc.org/github.com/timpalpant/gzran?status.svg)](http://godoc.org/github.com/timpalpant/gzran)
[![Build Status](https://travis-ci.org/timpalpant/gzran.svg?branch=master)](https://travis-ci.org/timpalpant/gzran)
[![Coverage Status](https://coveralls.io/repos/timpalpant/gzran/badge.svg?branch=master&service=github)](https://coveralls.io/github/timpalpant/gzran?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/timpalpant/gzran)](https://goreportcard.com/badge/github.com/timpalpant/gzran)

Package `gzran` reads arbitrary offsets of uncompresssed data from
compressed gzip files. This is accomplished by saving decompressor state periodically
as the Reader progresses on the fly. The built Index can then be used to seek
back to points within the file efficiently, or serialized and then used later.

Gzran is based on the c library, zran, by Mark Adler:
https://github.com/madler/zlib/blob/master/examples/zran.c

## Example
```go
gzr, _ := gzran.NewReader(r)
// Seek forward within the file, building index as we go.
if _, err := gzr.Seek(n, io.SeekStart); err != nil {
  panic(err)
}
// Seek backward, using the on-the-fly index to do so efficiently.
if _, err := gzr.Seek(n - 128000, io.SeekStart); err != nil {
  panic(err)
}

// Read through entire file to index it, and then save the Index to a file.
if _, err := io.Copy(ioutil.Discard, gzr); err != nil {
  panic(err)
}
if err := gzr.Index.WriteTo(f); err != nil {
  panic(err)
}

// Create a new gzip.Reader and load the Index to use it.
gzr, _ := gzran.NewReader(r)
gzr.Index, _ = gzran.LoadIndex(f)
// Seek and read as desired using the Index.
```
