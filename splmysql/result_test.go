package splmysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCopy(t *testing.T) {
	a := NewResult(1)
	a.Executed = 2
	a.Succeeded = 3
	a.Failed = 4
	a.RowsAffected = 5

	b := a.Copy()
	b.Plan = 6
	b.Executed = 7
	b.Succeeded = 8
	b.Failed = 9
	b.RowsAffected = 10

	assert.NotEqual(t, a.Plan, b.Plan)
	assert.NotEqual(t, a.Executed, b.Executed)
	assert.NotEqual(t, a.Succeeded, b.Succeeded)
	assert.NotEqual(t, a.Failed, b.Failed)
	assert.NotEqual(t, a.RowsAffected, b.RowsAffected)
}

func TestAppend(t *testing.T) {
	a := NewResult(0)
	b := NewResult(1)
	b.Executed = 2
	b.Succeeded = 3
	b.Failed = 4
	b.RowsAffected = 5
	a.Append(b)

	assert.Equal(t, a.Plan, int64(1))
	assert.Equal(t, a.Executed, int64(2))
	assert.Equal(t, a.Succeeded, int64(3))
	assert.Equal(t, a.Failed, int64(4))
	assert.Equal(t, a.RowsAffected, int64(5))
}
