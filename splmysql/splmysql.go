package splmysql

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/Sirupsen/logrus"

	// MySQL Driver
	_ "github.com/go-sql-driver/mysql"
	"github.com/sjmudd/mysql_defaults_file"
)

// Runner includes splmysql global variables.
type Runner struct {
	// DB is DB object
	db *sql.DB

	// Logger is logrus Logger object. You can overide it.
	Logger *logrus.Logger

	// DBName is the DB Name connected to
	DBName string

	// SplitRange is the max size of range used in splitted update
	SplitRange int64

	// LogLevel is loglevel of logger
	LogLevel int

	// UseDryRun is flag to enable dryrun mode
	UseDryRun bool

	// UseShuffle is flag to enable shuffle update mode.
	UseShuffle bool

	// Sessions is splmysql sessions handled by this Runner
	Sessions []*Session
}

// DefaultSplitRange is lower than 131072
// (default limit value in Galera Cluster's 'wsrep_max_ws_rows')
const DefaultSplitRange = int64(100000)

func newRunner(dbName string) (sr Runner) {
	sr = Runner{}
	sr.DBName = dbName
	sr.SetSplitRange(DefaultSplitRange)
	sr.LogLevel = LogDefaultLevel

	sr.initLogger()
	return
}

// NewByConf makes DB connection with my.cnf and returns Runner object.
func NewByConf(dbName string, conf string) (sr Runner, err error) {
	sr = newRunner(dbName)
	sr.db, err = mysql_defaults_file.OpenUsingDefaultsFile("mysql", conf, dbName)
	return
}

// NewByOptions makes DB connection with options and returns Runner object.
func NewByOptions(dbName string, host string, port int, user string, pwd string, charset string) (sr Runner, err error) {
	sr = newRunner(dbName)

	if charset == "" {
		charset = "utf8"
	}
	dbURI := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s", user, pwd, host, port, dbName, charset)
	sr.db, err = sql.Open("mysql", dbURI)
	return
}

// SetLogLevel sets loglevel of splmysql.
// Default, output nothing.
func (sr *Runner) SetLogLevel(level int) (oldValue int) {
	oldValue = sr.LogLevel
	sr.LogLevel = level
	switch sr.LogLevel {
	case 0:
		// disable output
		sr.Logger.Level = logrus.PanicLevel
	case 1:
		sr.Logger.Level = logrus.InfoLevel
	case 2:
		sr.Logger.Level = logrus.DebugLevel
	default:
		sr.Logger.Level = logrus.DebugLevel
	}

	return
}

// SetSplitRange updates split range used by Runner.Run()
func (sr *Runner) SetSplitRange(num int64) (oldValue int64) {
	if num <= 0 {
		panic("SetSplitRange needs integer value.")
	}
	oldValue = sr.SplitRange
	sr.SplitRange = num
	return
}

// Connected checks the DB connection whether active or not.
func (sr *Runner) Connected() bool {
	if sr.db != nil {
		if err := sr.db.Ping(); err == nil {
			return true
		}
	}
	return false
}

// Close disconnects the DB connection.
func (sr *Runner) Close() {
	sr.db.Close()
}

func (sr *Runner) getColumnDataForSplit(table string) (columnName string, minValue int64, maxValue int64, err error) {
	maxValue = -1

	query := fmt.Sprintf(`SHOW CREATE TABLE %s`, table)
	sr.tracef("Exec SQL: %s", query)

	var returnedTableName string
	var info string
	if err := sr.db.QueryRow(query).Scan(&returnedTableName, &info); err != nil {
		return "", -1, -1, err
	}

	if columnName = findColumnNameForSplit(info); columnName == "" {
		err = NewNoUsableColumnError(fmt.Sprintf("%s.%s", sr.DBName, table))
		return "", -1, -1, err
	}

	// search Max Value
	var min, max sql.NullInt64
	query = fmt.Sprintf(`SELECT MIN(%s), MAX(%s) FROM %s`, columnName, columnName, table)
	sr.tracef("Exec SQL: %s", query)
	if err := sr.db.QueryRow(query).Scan(&min, &max); err != nil {
		return "", -1, -1, err
	}

	if !min.Valid || !max.Valid {
		err = NewNoUsableColumnError(fmt.Sprintf("%s.%s", sr.DBName, table))
		return "", -1, -1, err
	}
	minValue = min.Int64
	maxValue = max.Int64
	return
}

func (sr *Runner) doUpdate(sql string) (rowsAffected int64, lastInsertID int64, err error) {
	sr.tracef("DEBUG: exec %s", sql)
	if !sr.UseDryRun {
		tx, err := sr.db.Begin()
		if err != nil {
			return 0, 0, err
		}
		defer func() {
			if err != nil {
				tx.Rollback()
				return
			}
			err = tx.Commit()
		}()

		result, err := tx.Exec(sql)
		if err != nil {
			return 0, 0, err
		}
		rowsAffected, _ = result.RowsAffected()
		lastInsertID, _ = result.LastInsertId()

		err = tx.Commit()
		if err != nil {
			return 0, 0, err
		}
	}
	return rowsAffected, lastInsertID, nil
}

// NewSession creates session data from query.
func (sr *Runner) NewSession(query string) (session *Session, err error) {
	execQuery := strings.Trim(query, " ;")
	if !isUpdateQuery(execQuery) {
		return session, NewInvalidUpdateQueryError("query must starts with 'UPDATE tablename SET ...'")
	}
	if isLimitedQuery(execQuery) {
		return session, NewInvalidUpdateQueryError("execute query has limit, its invalid")
	}

	tableName := getUpdateTableName(execQuery)
	if tableName == "" {
		return session, NewInvalidUpdateQueryError("query must starts with 'UPDATE tablename SET ...'")
	}

	columnName, min, max, err := sr.getColumnDataForSplit(tableName)
	if err != nil {
		return session, err
	} else if columnName == "" || min < 0 || max < 0 {
		return session, NewNoUsableColumnError(fmt.Sprintf("%s.%s", sr.DBName, tableName))
	}

	sr.debugf("[%s.%s] The column name to split is '%s': min '%d' - max '%d'",
		sr.DBName, tableName, columnName, min, max)

	// create transactions.
	transactions := []*Transaction{}
	for i := int64(0); i < max/sr.SplitRange+1; i++ {
		tx := Transaction{
			id:         i + 1,
			rangeStart: i * sr.SplitRange,
			rangeEnd:   (i+1)*sr.SplitRange - 1,
		}
		if tx.rangeEnd < min || tx.rangeStart > max {
			//   current values:    |------|
			// this transaction: |-|
			//               or             |-|
			// - this transaction is out of range. skip it.
			continue
		} else if tx.rangeStart < min {
			//   current values:    |------|
			// this transaction:  |---|
			tx.rangeStart = min
		}
		if tx.rangeEnd > max {
			//   current values:    |------|
			// this transaction:         |---|
			tx.rangeEnd = max
		}

		transactions = append(transactions, &tx)
	}

	if sr.UseShuffle {
		sr.debugf("[%s.%s] This session enable shuffle mode.", sr.DBName, tableName)
		shuffleTransactions(transactions)
	}

	// create split update session information
	session = &Session{
		Query:                    execQuery,
		DBName:                   sr.DBName,
		TableName:                tableName,
		SplittableColumn:         columnName,
		SplittableColumnMinValue: min,
		SplittableColumnMaxValue: max,
		SplitRange:               sr.SplitRange,
		transactions:             transactions,
		result:                   NewResult(int64(len(transactions))),
	}

	sr.debugf("[%s.%s] This session executes %d queries.",
		sr.DBName, tableName, len(session.transactions))

	return session, nil
}

// RunParallel executes session parallel
func (sr *Runner) RunParallel(sess *Session, parallel int) (retrySessionData *Session, err error) {
	// append Session
	sr.Sessions = append(sr.Sessions, sess)

	semaphore := make(chan struct{}, parallel)
	sr.db.SetMaxIdleConns(parallel)
	sr.db.SetMaxOpenConns(parallel)
	sr.db.SetConnMaxLifetime(0)

	sr.infof("[%s.%s] Session start (planned %d queries)", sess.DBName, sess.TableName, sess.result.Plan)

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
			sess.updateResult(err, rowsAffected)
			sr.infof("[%d] - Affected %d rows, total %d updated.", tx.id, rowsAffected, sess.result.RowsAffected)

			<-semaphore
			return nil
		}(transaction)
	}
	wg.Wait()
	close(semaphore)

	r := sess.GetSessionResult()
	if r.Failed > 0 {
		retrySessionData = &Session{
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

// SimpleUpdate executes UPDATE query simply, no modifies.
func (sr *Runner) SimpleUpdate(query string) (result Result, err error) {
	execQuery := strings.Trim(query, " ;")
	if !isUpdateQuery(execQuery) {
		return result, NewInvalidUpdateQueryError("execute query must start with 'UPDATE tablename SET ...'")
	}
	// create dummy session
	session := Session{
		Query:                    execQuery,
		DBName:                   sr.DBName,
		TableName:                "",
		SplittableColumn:         "",
		SplittableColumnMinValue: 0,
		SplittableColumnMaxValue: 0,
		SplitRange:               0,
		transactions:             nil,
		result:                   NewResult(1),
	}
	// append Session
	sr.Sessions = append(sr.Sessions, &session)

	rowsAffected, _, err := sr.doUpdate(execQuery)
	session.result.Executed = 1
	if err != nil {
		session.result.RowsAffected = 0
		session.result.Succeeded = 0
		session.result.Failed = 1

		return result, err
	}
	session.result.RowsAffected = rowsAffected
	session.result.Succeeded = 1
	session.result.Failed = 0

	sr.infof("[%s] Total %d rows updated.", sr.DBName, rowsAffected)
	sr.infof("[%s] Executed %d queries: %d succeeded, %d failed.",
		sr.DBName, result.Executed, result.Succeeded, result.Failed)

	return
}
