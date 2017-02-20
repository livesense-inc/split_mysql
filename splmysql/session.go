package splmysql

import (
	"fmt"
	"sync"
)

// Session is a data of splmysql parallel execution
type Session struct {
	runner                   *Runner
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

// updateResult updates sessionResult and Runner's TotalResult.
func (sess *Session) updateResult(err error, id int64, rowsAffected int64) error {
	sess.mutexResult.Lock()
	defer sess.mutexResult.Unlock()

	if err != nil {
		// update result
		sess.result.Executed++
		sess.result.Failed++
		return err
	}

	// update result
	sess.result.Executed++
	sess.result.Succeeded++
	sess.result.RowsAffected += rowsAffected
	// print result
	sess.runner.infof("[%d] - Affected %d rows, total %d updated.", id, rowsAffected, sess.result.RowsAffected)
	return nil
}

func (sess *Session) RunParallel(parallel int) (retrySessionData Session, err error) {
	sr := sess.runner

	// append Session
	sr.Sessions = append(sr.Sessions, sess)

	semaphore := make(chan struct{}, parallel)
	sr.db.SetMaxIdleConns(parallel)
	sr.db.SetMaxOpenConns(parallel)
	sr.db.SetConnMaxLifetime(0)

	sr.infof("[%s.%s] Session start", sess.DBName, sess.TableName)

	var wg sync.WaitGroup
	for _, transaction := range sess.transactions {
		wg.Add(1)
		semaphore <- struct{}{}
		go func(tx *Transaction) error {
			defer wg.Done()

			start := tx.rangeStart
			end := tx.rangeEnd
			updateSQL := getSplittedUpdateSQL(sess.Query, sess.SplittableColumn, start, end)
			sr.tracef("- (%d) update (range: %s = %d - %d) start",
				tx.id, sess.SplittableColumn, start, end)

			rowsAffected, _, err := sr.doUpdate(updateSQL)
			tx.completed = true
			if err != nil {
				sr.warnf("- (%d) ERROR: %s", tx.id, err.Error())
				tx.failed = true
			}
			sess.updateResult(err, tx.id, rowsAffected)
			<-semaphore
			return nil
		}(transaction)
	}
	wg.Wait()
	close(semaphore)
	sr.infof("[%s.%s] Session end", sess.DBName, sess.TableName)

	r := sess.GetSessionResult()
	if r.Failed > 0 {
		retrySessionData = Session{
			runner:                   sess.runner,
			Query:                    sess.Query,
			DBName:                   sess.DBName,
			TableName:                sess.TableName,
			SplittableColumn:         sess.SplittableColumn,
			SplittableColumnMinValue: sess.SplittableColumnMinValue,
			SplittableColumnMaxValue: sess.SplittableColumnMaxValue,
			SplitRange:               sess.SplitRange,
			transactions:             sess.GetFailedTransactions(),
			result:                   NewResult(int64(len(sess.GetFailedTransactions()))),
		}
		err = fmt.Errorf("[%s.%s] %d transactions failed\n", sess.DBName, sess.TableName, r.Failed)
		return
	}

	sr.infof("[%s.%s] Total %d rows updated.", sess.DBName, sess.TableName, r.RowsAffected)
	sr.infof("[%s.%s] Executed %d queries: %d succeeded, %d failed.",
		sess.DBName, sess.TableName, r.Executed, r.Succeeded, r.Failed)

	return
}
