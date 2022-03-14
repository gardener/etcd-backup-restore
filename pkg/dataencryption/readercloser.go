package dataencryption

import "io"

type readerCloser struct {
	r io.Reader
	c io.Closer
}

func (r *readerCloser) Read(p []byte) (n int, err error) {
	return r.r.Read(p)
}

func (r *readerCloser) Close() error {
	return r.c.Close()
}

var _ io.ReadCloser = &readerCloser{}
