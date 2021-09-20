package s3fs

import (
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/afero"
)

type S3Fs struct {
	client *s3.S3
}

func NewS3Fs(sess *session.Session) afero.Fs {
	c := s3.New(sess)

	return &S3Fs{
		client: c,
	}
}

func (fs *S3Fs) Create(name string) (afero.File, error) {
	return nil, nil
}

func (fs *S3Fs) Mkdir(name string, perm os.FileMode) error {
	return nil
}

func (fs *S3Fs) MkdirAll(name string, perm os.FileMode) error {
	return nil
}

func (fs *S3Fs) Open(name string) (afero.File, error) {
	return nil, nil
}

func (fs *S3Fs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	return nil, nil
}

func (fs *S3Fs) Remove(name string) error {
	return nil
}

func (fs *S3Fs) RemoveAll(name string) error {
	return nil
}

func (fs *S3Fs) Rename(oldname, newname string) error {
	return nil
}

func (fs *S3Fs) Stat(name string) (os.FileInfo, error) {
	return nil, nil
}

func (fs *S3Fs) Name() string {
	return ""
}

func (fs *S3Fs) Chmod(name string, mode os.FileMode) error {
	return nil
}

func (fs *S3Fs) Chown(name string, uid, gid int) error {
	return nil
}

func (fs *S3Fs) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return nil
}
