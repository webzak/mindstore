package testutil

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func dirFullPath(name string) (string, error) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("runtume caller path error")
	}
	fp := filepath.Dir(filename)
	return fp + "/" + name, nil
}

func CreateDir(name string) (string, error) {
	path, err := dirFullPath(name)
	if err != nil {
		return "", err
	}
	dir, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.Mkdir(path, 0775)
			if err != nil {
				return "", err
			}
		}
		return path, nil
	}
	if !dir.IsDir() {
		return "", errors.New(path + " is not directory")
	}
	return path, nil
}

func RemoveDir(name string) error {
	path, err := dirFullPath(name)
	if err != nil {
		return err
	}
	_, err = os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		return nil
	}
	return os.RemoveAll(path)
}

func SetupDir(name string) (string, error) {
	RemoveDir(name)
	return CreateDir(name)
}

func MakeTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "mindstore_test")
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func MakeTempFile(t *testing.T, pattern string) string {
	t.Helper()
	f, err := os.CreateTemp("", pattern)
	if err != nil {
		t.Fatal(err)
	}
	name := f.Name()
	f.Close()
	return name
}
