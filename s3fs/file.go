package s3fs

import (
	"errors"
	// "fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	ErrFileClosed        = errors.New("File is closed")
	ErrOutOfRange        = errors.New("Out of range")
	ErrTooLarge          = errors.New("Too large")
	ErrFileNotFound      = os.ErrNotExist
	ErrFileExists        = os.ErrExist
	ErrDestinationExists = os.ErrExist
)

type File struct {
	sync.Mutex
	fs     *S3Fs
	name   string
	closed bool
	at     int64
	data   []byte
}

type FileInfo struct {
	name *string
	size *int64
}

func (f *File) Close() error {
	return nil
}

func (f *File) Read(b []byte) (n int, err error) {
	f.Mutex.Lock()
	defer f.Mutex.Unlock()

	if f.closed {
		return 0, ErrFileClosed
	}

	if len(b) > 0 && int(f.at) == len(f.data) {
		return 0, io.EOF
	}

	if int(f.at) > len(f.data) {
		return 0, io.ErrUnexpectedEOF
	}

	if len(f.data)-int(f.at) >= len(b) {
		n = len(b)
	} else {
		n = len(f.data) - int(f.at)
	}

	copy(b, f.data[f.at:f.at+int64(n)])
	atomic.AddInt64(&f.at, int64(n))

	return
}

func (f *File) ReadAt(b []byte, off int64) (n int, err error) {
	prev := atomic.LoadInt64(&f.at)
	atomic.StoreInt64(&f.at, off)
	n, err = f.Read(b)
	atomic.StoreInt64(&f.at, prev)
	return
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}

func (f *File) Write(b []byte) (n int, err error) {
	return 0, nil
}

func (f *File) WriteAt(b []byte, off int64) (n int, err error) {
	return 0, nil
}

func (f *File) Name() string { return f.name }

func (f *File) Readdir(n int) ([]fs.FileInfo, error) {
	fi := make([]fs.FileInfo, 0)

	in := &s3.ListObjectsV2Input{
		Bucket: aws.String(f.fs.bucket),
		Prefix: aws.String(f.name),
	}

	var count int
	err := f.fs.s3.ListObjectsV2Pages(in, func(page *s3.ListObjectsV2Output, last bool) bool {
		for _, o := range page.Contents {
			count++

			fi = append(fi, &FileInfo{o.Key, o.Size})

			if count > n && n > 0 {
				return false
			}
		}

		return !last
	})
	if err != nil {
		return fi, err
	}

	return fi, nil
}

func (f *File) Readdirnames(n int) ([]string, error) {
	fi, err := f.Readdir(n)
	if err != nil {
		return nil, err
	}

	files := make([]string, 0)
	for _, file := range fi {
		files = append(files, file.Name())
	}

	return files, nil
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

func (f *FileInfo) Name() string { return *f.name }

func (f *FileInfo) Size() int64 { return *f.size }

func (f *FileInfo) Mode() fs.FileMode {
	return 0
}

func (f *FileInfo) ModTime() time.Time {
	return time.Now()
}

func (f *FileInfo) IsDir() bool {
	_, file := path.Split(*f.name)
	if file != "" {
		return false
	}

	return true
}

func (f *FileInfo) Sys() interface{} {
	return nil
}
