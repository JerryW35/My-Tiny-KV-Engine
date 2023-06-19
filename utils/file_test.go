package utils

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDirSize(t *testing.T) {
	dir, _ := os.Getwd()
	t.Log(dir)
	dirSize, err := DirSize(dir)
	assert.Nil(t, err)
	t.Log(dirSize)
	assert.True(t, dirSize > 0)
}
func TestAvailableDiskSize(t *testing.T) {
	size, err := AvailableDiskSize()
	assert.Nil(t, err)
	assert.True(t, size > 0)
}
