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
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/afero"
)

// S3Fs is a Fs implementation for S3 like storages
type S3Fs struct {
	s3     *s3.S3
	bucket string
}

func NewS3Fs(sess *session.Session, bucket string) afero.Fs {
	c := s3.New(sess)

	return &S3Fs{
		s3:     c,
		bucket: bucket,
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
	f := &File{
		fs:   fs,
		name: name,
	}

	return f, nil
}

// OpenFile same as Open method, because S3 doesn't support access permissions and open flags
func (fs *S3Fs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	return fs.Open(name)
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
