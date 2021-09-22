package s3fs

import (
	"errors"
	// "fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"sync"
	// "sync/atomic"
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
	fs   *S3Fs
	name string
	// closed bool
	at int64
	// data   []byte
	out *s3.GetObjectOutput
}

type FileInfo struct {
	name *string
	size *int64
}

func (f *File) Close() error {
	return nil
}

func (f *File) Read(b []byte) (n int, err error) {
	if f.out == nil {
		var err error
		in := &s3.GetObjectInput{
			Bucket: aws.String(f.fs.bucket),
			Key:    aws.String(f.name),
		}

		f.out, err = f.fs.s3.GetObject(in)
		if err != nil {
			return 0, err
		}
	}

	n, err = f.out.Body.Read(b)
	f.at += int64(n)

	return
}

func (f *File) ReadAt(b []byte, off int64) (n int, err error) {
	// skip bytes from begining of File
	// until ofset will not equal io.Reader internal offset
	var p []byte
	for {
		switch {
		case off < 0:
			return 0, io.EOF
		case f.at+int64(len(b)) < off:
			p = make([]byte, len(b))
		case f.at < off && f.at+int64(len(b)) >= off:
			p = make([]byte, off-f.at)
		default:
			p = make([]byte, 0)
		}

		if len(p) == 0 {
			break
		}

		n, err = f.Read(p)
		f.at += int64(n)
		if err != nil {
			return 0, err
		}
	}

	n, err = f.Read(b)
	f.at += int64(n)

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
