// Copyright © 2015 Steve Francia <spf@spf13.com>.
// Copyright 2013 tsuru authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package s3fs

import (
	"fmt"
	"os"
	"path"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/afero"
)

// S3Fs is a Fs implementation for S3 like storages
type S3Fs struct {
	s3     *s3.S3
	bucket string
	opts   *S3FsOpts
}

type S3FsOpts struct {
	minPartSize int64
}

func NewS3Fs(sess *session.Session, bucket string, opts *S3FsOpts) afero.Fs {
	if opts == nil {
		opts = &S3FsOpts{
			minPartSize: int64(5 * 1024 * 1024),
		}
	}

	c := s3.New(sess)

	return &S3Fs{
		s3:     c,
		bucket: bucket,
		opts:   opts,
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
	return fs.OpenFile(name, os.O_RDONLY, 0)
}

func (fs *S3Fs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	f := &File{
		fs:   fs,
		name: name,
		flag: flag,
	}

	if flag&os.O_TRUNC != 0 {
		err := f.Truncate(0)
		if err != nil {
			return nil, err
		}
	}

	if flag&os.O_CREATE == 0 {
		_, err := f.Stat()
		if err != nil {
			return nil, err
		}
	}

	return f, nil
}

func (fs *S3Fs) Remove(name string) error {
	_, err := fs.Open(name)
	if err != nil {
		return err
	}

	in := &s3.DeleteObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(name),
	}

	_, err = fs.s3.DeleteObject(in)
	if err != nil {
		return err
	}

	return nil
}

func (fs *S3Fs) RemoveAll(name string) error {
	return nil
}

func (fs *S3Fs) Rename(oldname, newname string) error {
	c := &s3.CopyObjectInput{
		Bucket:     aws.String(fs.bucket),
		Key:        aws.String(newname),
		CopySource: aws.String(path.Join(fs.bucket, oldname)),
	}

	_, err := fs.s3.CopyObject(c)
	if err != nil {
		return err
	}

	d := &s3.DeleteObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(oldname),
	}

	_, err = fs.s3.DeleteObject(d)
	if err != nil {
		return err
	}

	return nil
}

func (fs *S3Fs) Stat(name string) (os.FileInfo, error) {
	return nil, nil
}

func (fs *S3Fs) Name() string { return fmt.Sprintf("s3://%s", fs.bucket) }

func (fs *S3Fs) Chmod(name string, mode os.FileMode) error {
	return nil
}

func (fs *S3Fs) Chown(name string, uid, gid int) error {
	return nil
}

func (fs *S3Fs) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return nil
}
