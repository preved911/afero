package s3fs

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	// "github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	ErrFileClosed        = errors.New("File is closed")
	ErrOutOfRange        = errors.New("Out of range")
	ErrTooLarge          = errors.New("Too large")
	ErrFileNotFound      = os.ErrNotExist
	ErrFileExists        = os.ErrExist
	ErrDestinationExists = os.ErrExist

	minPartSize = int(5 * 1024 * 1024)
)

type File struct {
	sync.Mutex
	fs     *S3Fs
	name   string
	closed bool
	fileUpload
	fileDownload
}

type fileUpload struct {
	body      []byte
	multipart *fileUploadMultipart
}

type fileUploadMultipart struct {
	parts []*s3.CompletedPart
	out   *s3.CreateMultipartUploadOutput
}

type fileDownload struct {
	off int64
	out *s3.GetObjectOutput
}

type FileInfo struct {
	name *string
	size *int64
}

func (f *File) Close() error {
	if f.closed {
		return ErrFileClosed
	}

	err := f.Sync()
	if err != nil {
		return err
	}

	f.fileDownload.out = nil
	f.fileDownload.off = 0

	f.fileUpload.body = make([]byte, 0)
	f.fileUpload.multipart = nil

	f.closed = true

	return nil
}

func (f *File) Read(b []byte) (n int, err error) {
	// we should get file body from remote storage
	if f.fileDownload.out == nil {
		in := &s3.GetObjectInput{
			Bucket: aws.String(f.fs.bucket),
			Key:    aws.String(f.name),
		}

		f.fileDownload.out, err = f.fs.s3.GetObject(in)
		if err != nil {
			return 0, err
		}
	}

	n, err = f.fileDownload.out.Body.Read(b)
	f.fileDownload.off += int64(n)

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
		case f.fileDownload.off+int64(len(b)) < off:
			p = make([]byte, len(b))
		case f.fileDownload.off < off && f.fileDownload.off+int64(len(b)) >= off:
			p = make([]byte, off-f.fileDownload.off)
		default:
			p = make([]byte, 0)
		}

		if len(p) == 0 {
			break
		}

		n, err = f.Read(p)
		if err != nil {
			return 0, err
		}
	}

	n, err = f.Read(b)

	return
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}

func (f *File) Write(b []byte) (n int, err error) {
	if len(b) >= minPartSize || f.fileUpload.multipart != nil {
		if f.fileUpload.multipart == nil {
			f.fileUpload.multipart = &fileUploadMultipart{}

			ft := http.DetectContentType(b)

			in := &s3.CreateMultipartUploadInput{
				Bucket:      aws.String(f.fs.bucket),
				Key:         aws.String(f.name),
				ContentType: aws.String(ft),
			}

			f.fileUpload.multipart.out, err = f.fs.s3.CreateMultipartUpload(in)
			if err != nil {
				return 0, err
			}

			f.fileUpload.multipart.parts = make([]*s3.CompletedPart, 0)
		}

		partNumber := int64(len(f.fileUpload.multipart.parts) + 1)
		contentLength := int64(len(b))

		pi := &s3.UploadPartInput{
			Bucket:        f.fileUpload.multipart.out.Bucket,
			Key:           f.fileUpload.multipart.out.Key,
			UploadId:      f.fileUpload.multipart.out.UploadId,
			Body:          bytes.NewReader(b),
			PartNumber:    aws.Int64(partNumber),
			ContentLength: aws.Int64(contentLength),
		}

		res, err := f.fs.s3.UploadPart(pi)
		if err != nil {
			return 0, err
		} else {
			f.fileUpload.multipart.parts = append(
				f.fileUpload.multipart.parts,
				&s3.CompletedPart{
					ETag:       res.ETag,
					PartNumber: aws.Int64(partNumber),
				},
			)
		}
	} else {
		f.fileUpload.body = b
	}

	return int(len(b)), nil
}

func (f *File) WriteAt(b []byte, off int64) (n int, err error) {
	if f.fileDownload.out == nil || f.fileDownload.off < int64(len(b)) {
		var p []byte
		for {
			switch {
			case off < 0:
				return 0, io.EOF
			case int64(len(b))-f.fileDownload.off > int64(minPartSize):
				p = make([]byte, minPartSize)
			case int64(len(b))-f.fileDownload.off > 0:
				p = make([]byte, len(b))
			default:
				p = make([]byte, 0)
			}

			if len(p) == 0 {
				break
			}

			n, err = f.Read(p)
			if err != nil {
				return 0, err
			}

			f.fileUpload.body = append(f.fileUpload.body, p...)
		}
	}

	return f.Write(f.fileUpload.body)
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
	if f.fileUpload.multipart != nil {
		if f.fileUpload.multipart.out != nil {
			in := &s3.CompleteMultipartUploadInput{
				Bucket:   f.fileUpload.multipart.out.Bucket,
				Key:      f.fileUpload.multipart.out.Key,
				UploadId: f.fileUpload.multipart.out.UploadId,
				MultipartUpload: &s3.CompletedMultipartUpload{
					Parts: f.fileUpload.multipart.parts,
				},
			}

			_, err := f.fs.s3.CompleteMultipartUpload(in)

			return err
		}
	} else {
		in := &s3.PutObjectInput{
			Bucket: aws.String(f.fs.bucket),
			Key:    aws.String(f.name),
			Body:   bytes.NewReader(f.fileUpload.body),
		}

		_, err := f.fs.s3.PutObject(in)

		return err
	}

	return nil
}

func (f *File) Truncate(size int64) error {
	var b []byte
	for {
		switch {
		case size < 0:
			return io.EOF
		case size-f.fileDownload.off > int64(minPartSize):
			b = make([]byte, minPartSize)
		case size-f.fileDownload.off > 0:
			b = make([]byte, len(b))
		default:
			b = make([]byte, 0)
		}

		if len(b) == 0 {
			break
		}

		_, err := f.Read(b)
		if err != nil {
			return err
		}

		f.fileUpload.body = append(f.fileUpload.body, b...)
	}

	_, err := f.Write(f.fileUpload.body)

	return err
}

func (f *File) WriteString(s string) (n int, err error) { return f.Write([]byte(s)) }

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
