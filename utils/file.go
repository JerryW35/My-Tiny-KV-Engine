package utils

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// get the size of a directory
func DirSize(dir string) (int64, error) {
	var size int64
	err := filepath.Walk(dir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

func AvailableDiskSize() (uint64, error) {
	wd, err := syscall.Getwd()
	if err != nil {
		return 0, err
	}
	var stat syscall.Statfs_t
	if err = syscall.Statfs(wd, &stat); err != nil {
		return 0, err
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}

// copy dir
func CopyDir(src string, dest string, exclude []string) error {
	// create dest dir
	err := os.MkdirAll(dest, os.ModePerm)
	if err != nil {
		return fmt.Errorf("无法创建目标目录: %s", err)
	}

	// read src dir
	filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		fileName := strings.Replace(path, src, "", 1)
		if fileName == "" {
			return nil
		}

		// check exclude file
		base := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
		for _, name := range exclude {
			if base == name {
				if info.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
		}

		// copy file
		destPath := filepath.Join(dest, info.Name())

		if info.IsDir() {
			err := os.MkdirAll(destPath, os.ModePerm)
			if err != nil {
				return err
			}
		} else {
			srcFile, err := os.Open(path)
			if err != nil {
				return err
			}
			defer srcFile.Close()

			destFile, err := os.Create(destPath)
			if err != nil {
				return err
			}
			defer destFile.Close()

			_, err = io.Copy(destFile, srcFile)
			if err != nil {
				return err
			}
		}

		return nil
	})

	return nil
}
