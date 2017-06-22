package splmysql

// Result is struct of SQL execution result.
type Result struct {
	// Plan is number of estimated to execute
	Plan int64
	// Executed is number of queries executed to DB.
	Executed int64
	// Succeeded is number of queries succeeded.
	Succeeded int64
	// Failed is number of queries failed.
	Failed int64
	// RowsAffected is number of rows updated.
	RowsAffected int64
	// LastInsertID is auto_increment last id. Not used in this implementation.
	//LastInsertID int64
}

// NewResult returns struct of New Result
func NewResult(plan int64) Result {
	return Result{
		Plan:         plan,
		Executed:     0,
		Succeeded:    0,
		Failed:       0,
		RowsAffected: 0,
	}
}

// Copy returns self copy struct
func (r *Result) Copy() Result {
	return Result{
		Plan:         r.Plan,
		Executed:     r.Executed,
		Succeeded:    r.Succeeded,
		Failed:       r.Failed,
		RowsAffected: r.RowsAffected,
	}
}

// Append appends another Result.
func (r *Result) Append(result Result) {
	r.Plan += result.Plan
	r.Executed += result.Executed
	r.Succeeded += result.Succeeded
	r.Failed += result.Failed
	r.RowsAffected += result.RowsAffected
}
