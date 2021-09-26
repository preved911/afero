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

	// err := f.Sync()
	// if err != nil {
	// 	return err
	// }

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
		f.fileDownload.out, err = f.getObject()
		if err != nil {
			return 0, err
		}
	}

	n, err = f.fileDownload.out.Body.Read(b)
	f.fileDownload.off += int64(n)

	return
}

func (f *File) ReadAt(b []byte, off int64) (n int, err error) {
	// change offset before reading file
	_, err = f.Seek(off, 0)
	if err != nil {
		return
	}

	return f.Read(b)
}

func (f *File) Seek(offset int64, whence int) (int64, error) {
	var b []byte
	var err error

	if f.fileDownload.out == nil {
		f.fileDownload.out, err = f.getObject()
		if err != nil {
			return 0, err
		}
	}

	stat, err := f.Stat()
	if err != nil {
		return 0, err
	}
	size := stat.Size()

	for {
		switch whence {
		case 0:
			// relative to the origin of the file
			switch {
			case offset < 0:
				return 0, io.EOF
			case offset > size:
				return 0, io.EOF
			case offset < f.fileDownload.off:
				f.fileDownload.off = 0
				f.fileDownload.out, err = f.getObject()
				if err != nil {
					return 0, err
				}
			case offset-f.fileDownload.off < f.fs.opts.minPartSize:
				b = make([]byte, offset-f.fileDownload.off)
			case offset-f.fileDownload.off >= f.fs.opts.minPartSize:
				b = make([]byte, f.fs.opts.minPartSize)
			}
		case 1:
			// relative to the current offset
			switch {
			case offset < 0 && f.fileDownload.off+offset < 0:
				return 0, io.EOF
			case offset+f.fileDownload.off > size:
				return 0, io.EOF
			case offset < 0:
				// we must read from beginning of the file
				offset = f.fileDownload.off + offset
				whence = 0
				f.fileDownload.off = 0
				f.fileDownload.out, err = f.getObject()
				if err != nil {
					return 0, err
				}
			case offset < f.fs.opts.minPartSize:
				b = make([]byte, offset)
				f.fileDownload.off += offset
				offset = 0
			case offset >= f.fs.opts.minPartSize:
				b = make([]byte, f.fs.opts.minPartSize)
				f.fileDownload.off += f.fs.opts.minPartSize
				offset -= f.fs.opts.minPartSize
			}
		case 2:
			// relative to the end
			switch {
			case offset >= 0:
				return 0, io.EOF
			case -offset >= size:
				return 0, io.EOF
			default:
				// now read from the beggining of the file
				offset = size + offset
				whence = 0
				f.fileDownload.off = 0
				f.fileDownload.out, err = f.getObject()
				if err != nil {
					return 0, err
				}
			}
		}

		if len(b) == 0 {
			break
		}

		_, err := f.Read(b)
		if err != nil {
			return 0, err
		}

		f.fileUpload.body = append(f.fileUpload.body, b...)
		if int64(len(f.fileUpload.body)) >= f.fs.opts.minPartSize {
			_, err := f.Write(f.fileUpload.body[:f.fs.opts.minPartSize])
			if err != nil {
				return 0, err
			}

			f.fileUpload.body = f.fileUpload.body[f.fs.opts.minPartSize:]
		}
	}

	return f.fileDownload.off, nil
}

func (f *File) Write(b []byte) (n int, err error) {
	if int64(len(b)) >= f.fs.opts.minPartSize || f.fileUpload.multipart != nil {
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
	// change offset before writing file
	_, err = f.Seek(off, 0)
	if err != nil {
		return
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

func (f *File) Stat() (fs.FileInfo, error) {
	out, err := f.getHeadObjectOutput()
	if err != nil {
		return nil, err
	}

	fi := &FileInfo{
		name: &f.name,
		size: out.ContentLength,
	}

	return fi, nil
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
		case size-f.fileDownload.off > int64(f.fs.opts.minPartSize):
			b = make([]byte, f.fs.opts.minPartSize)
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

func (f *File) getObjectOutput() (*s3.GetObjectOutput, error) {
	in := &s3.GetObjectInput{
		Bucket: aws.String(f.fs.bucket),
		Key:    aws.String(f.name),
	}

	return f.fs.s3.GetObject(in)
}

func (f *File) getHeadObjectOutput() (*s3.GetObjectOutput, error) {
	in := &s3.HeadObjectInput{
		Bucket: aws.String(f.fs.bucket),
		Key:    aws.String(f.name),
	}

	return f.fs.s3.HeadObject(in)
}
