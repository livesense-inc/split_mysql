package splmysql

import "fmt"

// NoUsableColumnErrorCode is the exit code of NoUsableColumnError
const (
	InvalidUpdateQueryErrorCode = 10
	NoUsableColumnErrorCode     = 11
)

// ErrorInterface is generic interface of splmysql errors.
var SplErrorInterface interface {
	error
	//Error() string
	Code() int
}

// SplError is generic struct of splmysql errors.
type SplError struct {
	error
	exitcode int
}

// Code returns Error Code.
func (err *SplError) Code() int {
	return err.exitcode
}

// NoUsableColumnError is the error that it found no usable column for split update in the table.
type NoUsableColumnError struct {
	SplError
}

// NewNoUsableColumnError create NoUsableColumnError
func NewNoUsableColumnError(tableName string) *NoUsableColumnError {
	var err NoUsableColumnError
	err.exitcode = NoUsableColumnErrorCode
	if tableName == "" {
		err.error = fmt.Errorf("Cannot detect any usable column for split update the table\n")
		return &err
	}
	err.error = fmt.Errorf("Cannot detect any usable column for split update the table '%s'\n", tableName)
	return &err
}

// InvalidUpdateQueryError is the error that splmysql cannot treat.
type InvalidUpdateQueryError struct {
	SplError
}

// NewInvalidUpdateQueryError create InvalidUpdateQueryError.
func NewInvalidUpdateQueryError(hint string) *InvalidUpdateQueryError {
	var err InvalidUpdateQueryError
	err.exitcode = InvalidUpdateQueryErrorCode
	err.error = fmt.Errorf("%s\n", hint)
	return &err
}
