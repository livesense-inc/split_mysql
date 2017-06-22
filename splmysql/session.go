package splmysql

import "sync"

// Session is a data of splmysql parallel execution
type Session struct {
	Query                    string
	DBName                   string
	TableName                string
	SplittableColumn         string
	SplittableColumnMinValue int64
	SplittableColumnMaxValue int64
	SplitRange               int64
	transactions             []*Transaction
	result                   Result
	mutexResult              sync.RWMutex
}

// Transaction is single transaction data, equals to single SQL
type Transaction struct {
	id         int64
	completed  bool
	failed     bool
	rangeStart int64
	rangeEnd   int64
}

// GetSessionResult returns copy of session result data.
func (sess *Session) GetSessionResult() Result {
	sess.mutexResult.RLock()
	defer sess.mutexResult.RUnlock()
	return sess.result.Copy()
}

func (sess *Session) GetFailedTransactions() []*Transaction {
	transactions := []*Transaction{}
	for _, tx := range sess.transactions {
		if !tx.completed || !tx.failed {
			continue
		}
		t := &tx
		transactions = append(transactions, *t)
	}
	return transactions
}

// updateResult updates sessionResult.
func (sess *Session) updateResult(err error, rowsAffected int64) error {
	sess.mutexResult.Lock()
	defer sess.mutexResult.Unlock()

	if err != nil {
		sess.result.Executed++
		sess.result.Failed++
		return err
	}

	sess.result.Executed++
	sess.result.Succeeded++
	sess.result.RowsAffected += rowsAffected
	return nil
}
