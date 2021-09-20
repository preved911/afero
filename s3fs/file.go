package s3fs

import (
	"os"
)

type File struct {
	fs   S3Fs
	name string
	// isdir, closed bool
	// buf []byte
}

func (f *File) Close() error {
	return nil
}

func (f *File) Read(p []byte) (n int, err error) {
	return 0, nil
}

func (f *File) ReadAt(p []byte, off int64) (n int, err error) {
	return 0, nil
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}

func (f *File) Write(p []byte) (n int, err error) {
	return 0, nil
}

func (f *File) WriteAt(p []byte, off int64) (n int, err error) {
	return 0, nil
}

func (f *File) Name() string {
	return ""
}

func (f *File) Readdir(count int) ([]os.FileInfo, error) {
	return nil, nil
}

func (f *File) Readdirnames(n int) ([]string, error) {
	return nil, nil
}

func (f *File) Stat() (os.FileInfo, error) {
	return nil, nil
}

func (f *File) Sync() error {
	return nil
}

func (f *File) Truncate(size int64) error {
	return nil
}

func (f *File) WriteString(s string) (ret int, err error) {
	return 0, nil
}
