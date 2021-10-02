package s3fs

import (
	"io"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/spf13/afero"
)

var afs afero.Fs

func init() {
	opts := session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}

	sess, err := session.NewSessionWithOptions(opts)
	if err != nil {
		panic(err)
	}

	bucket := os.Getenv("AFERO_S3_BUCKET")
	if bucket == "" {
		os.Exit(1)
	}

	afs = NewS3Fs(sess, bucket, nil)
}

func eq(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}

	for i, v := range a {
		if v != b[i] {
			return false
		}
	}

	return true
}

var writeTests = []struct {
	name    string
	body    []byte
	flag, n int
	err     error
}{
	{
		"test1.txt",
		[]byte("1234567890"),
		os.O_CREATE | os.O_RDWR,
		10,
		nil,
	},
	{
		"test2.txt",
		[]byte("1234567890"),
		os.O_RDWR,
		0,
		ErrNotExist,
	},
	{
		"test1.txt",
		[]byte("1234567890"),
		os.O_RDONLY,
		0,
		ErrReadOnly,
	},
	{
		"test3.txt",
		[]byte("abc"),
		os.O_CREATE | os.O_WRONLY,
		3,
		nil,
	},
}

func TestFileWrite(t *testing.T) {
	for _, test := range writeTests {
		f, err := afs.OpenFile(test.name, test.flag, 0)
		if err != nil {
			if test.err != err {
				t.Errorf("failer to open file %s: %s", test.name, err)
			} else {
				continue
			}
		}

		n, err := f.Write(test.body)
		if err != nil {
			if test.err != err {
				t.Errorf("failed to write file %s: %s", test.name, err)
			} else {
				continue
			}
		}
		if test.n != n {
			t.Errorf("incorrect bytes count written %s %d, need %d", test.name, n, test.n)
		}

		err = f.Sync()
		if err != nil {
			if test.err != err {
				t.Errorf("failed to sync file %s: %s", test.name, err)
			} else {
				continue
			}
		}
	}
}

var readTests = []struct {
	name    string
	body    []byte
	flag, n int
	err     error
}{
	{
		"test1.txt",
		[]byte("1234567890"),
		os.O_CREATE | os.O_RDWR,
		10,
		nil,
	},
	{
		"test2.txt",
		[]byte("1234567890"),
		os.O_RDWR,
		0,
		ErrNotExist,
	},
	{
		"test3.txt",
		[]byte("abc"),
		os.O_WRONLY,
		0,
		ErrWriteOnly,
	},
}

func TestFileRead(t *testing.T) {
	for _, test := range readTests {
		f, err := afs.OpenFile(test.name, test.flag, 0)
		if err != nil {
			if test.err != err {
				t.Errorf("failer to open file %s: %s", test.name, err)
			} else {
				continue
			}
		}

		b := make([]byte, test.n)
		n, err := f.Read(b)
		if err != nil && err != io.EOF {
			if test.err != err {
				t.Errorf("failed to read file %s: %s", test.name, err)
			} else {
				continue
			}
		}
		if test.n != n {
			t.Errorf("incorrect bytes count readed %s %d, need %d", test.name, n, test.n)
		}
		if !eq(test.body, b) {
			t.Errorf("incorrect body readed %s %v, need %v", test.name, b, test.body)
		}
	}
}
