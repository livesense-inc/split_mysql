package main

import (
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"time"

	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/gosuri/uiprogress"
	"github.com/livesense-inc/split_mysql/splmysql"
	"gopkg.in/urfave/cli.v1"
)

// DefaultMyCnfPath is default path of my.cnf
const DefaultMyCnfPath = "~/.my.cnf"

var globalFlags = []cli.Flag{
	cliSuppressOutput,
	cliVerbose,
	cliDebug,
	cliTrace,
	cliDryRun,
	cliParallel,
	cliMaxRetry,
	cliShuffle,
	cliSplit,
	cliFallback,
	cliMyCnf,
	cliDBName,
	cliDBHost,
	cliDBPort,
	cliDBUser,
	cliDBPassword,
	cliExecute,
	cliDefaultCharSet,
}

var cliSuppressOutput = cli.BoolFlag{
	Name:  "suppress",
	Usage: "Suppress information. Output result and errors only.",
}

var cliVerbose = cli.BoolFlag{
	Name:  "verbose, v",
	Usage: "Enable verbose output.",
}

var cliDebug = cli.BoolFlag{
	Name:  "debug",
	Usage: "Enable debug output.",
}

var cliTrace = cli.BoolFlag{
	Name:  "trace",
	Usage: "Enable debug-trace output.",
}

var cliDryRun = cli.BoolFlag{
	Name:  "dryrun, n",
	Usage: "Enable dryrun, don't update DB.",
}

var cliParallel = cli.IntFlag{
	Name:  "parallel",
	Usage: "Parallel execution.",
	Value: 1,
}

var cliMaxRetry = cli.IntFlag{
	Name:  "max-retry",
	Usage: "Set max retries for query execution.",
	Value: 3,
}

var cliSplit = cli.Int64Flag{
	Name:  "split",
	Usage: "Split UPDATE SQL based on this value.",
	Value: splmysql.DefaultSplitRange,
}

var cliShuffle = cli.BoolFlag{
	Name:  "shuffle",
	Usage: "Shuffle splitted UPDATE SQL execution.",
}

var cliFallback = cli.BoolFlag{
	Name:  "fallback",
	Usage: "Fallback simple UPDATE if it cannot split. Use carefully if DB is Galera Cluster.",
}

/*
 Following options similar to mysql command
*/
var cliDBName = cli.StringFlag{
	Name:  "database, D",
	Usage: "DB name.",
}

var cliMyCnf = cli.StringFlag{
	Name:  "defaults-file",
	Usage: "Use this my.cnf as DB information.",
	Value: DefaultMyCnfPath,
}

var cliDBHost = cli.StringFlag{
	Name:   "host, h",
	Usage:  "DB host address.",
	EnvVar: "MYSQL_HOST",
}

var cliDBPort = cli.IntFlag{
	Name:   "port, P",
	Usage:  "DB port.",
	EnvVar: "MYSQL_TCP_PORT",
	Value:  3306,
}

var cliDBUser = cli.StringFlag{
	Name:   "user, u",
	Usage:  "DB user.",
	EnvVar: "MYSQL_USER",
}

var cliDBPassword = cli.StringFlag{
	Name:   "password, p",
	Usage:  "DB password.",
	EnvVar: "MYSQL_PWD",
}

var cliExecute = cli.StringFlag{
	Name:  "execute, e",
	Usage: "UPDATE query.",
}

var cliDefaultCharSet = cli.StringFlag{
	Name:  "default-character-set",
	Usage: "Set default character set.",
	Value: "utf8",
}

// create logger
var logger = logrus.New()

func doUpdate(sr *splmysql.Runner, sessionData *splmysql.Session, parallel int, maxRetry int, cnt int) (err error) {
	// execute parallel
	retrySessionData, err := sr.RunParallel(sessionData, parallel)
	// retry
	if err != nil {
		logger.Warnf("Session %d failed: %s\n", cnt, err.Error())
		logger.Debugf("Retry %d/%d: execute %d transactions.",
			cnt+1, maxRetry, retrySessionData.GetSessionResult().Plan)

		return doUpdate(sr, retrySessionData, parallel, maxRetry, cnt+1)
	}
	return
}

func doMain(c *cli.Context) (err error) {
	logger.Formatter = &logrus.TextFormatter{
		FullTimestamp: false,
	}
	logger.Out = os.Stdout

	// split commandline args
	mycnf := c.String("conf")
	host := c.String("host")
	port := c.Int("port")
	user := c.String("user")
	pwd := c.String("password")
	charset := c.String("default-character-set")

	dbName := c.String("database")
	sql := c.String("execute")

	// Load splmysql
	var sr splmysql.Runner
	if host == "" {
		sr, err = splmysql.NewByConf(dbName, mycnf)
		if err != nil {
			return err
		}
	} else {
		sr, err = splmysql.NewByOptions(dbName, host, port, user, pwd, charset)
		if err != nil {
			return err
		}
	}
	defer sr.Close()

	// set parameters
	sr.UseDryRun = c.Bool("dryrun")
	sr.SetSplitRange(c.Int64("split"))
	sr.UseShuffle = c.Bool("shuffle")

	fallback := c.Bool("fallback")
	parallel := c.Int("parallel")
	maxretry := c.Int("max-retry")

	showProgress := false
	if c.Bool("suppress") {
		logger.Level = logrus.ErrorLevel
		// splmysql use library mode logging.
		sr.SetLogLevel(splmysql.LogDefaultLevel)
	} else if c.Bool("verbose") {
		logger.Level = logrus.InfoLevel
		sr.SetLogLevel(splmysql.LogInfoLevel)
	} else if c.Bool("debug") {
		logger.Level = logrus.DebugLevel
		sr.SetLogLevel(splmysql.LogDebugLevel)
	} else if c.Bool("trace") {
		logger.Level = logrus.DebugLevel
		sr.SetLogLevel(splmysql.LogTraceLevel)
	} else {
		showProgress = true
		logger.Level = logrus.WarnLevel
		sr.SetLogLevel(splmysql.LogDefaultLevel)
	}

	// Overide Logger
	sr.Logger = logger

	var wg sync.WaitGroup
	errChan := make(chan error, 1)
	wg.Add(1)
	go func() error {
		defer wg.Done()

		// Create session. If error occures, return simply
		sess, err := sr.NewSession(sql)
		if err != nil {
			errChan <- err
			return err
		}
		errChan <- doUpdate(&sr, sess, parallel, maxretry, 0)
		return nil
	}()

	if !showProgress {
		err = <-errChan
		wg.Wait()
	} else {
		// Drow progress bar
		uiprogress.Start()
		pBars := map[int]*uiprogress.Bar{}

		updateProgressbar := func() {
			for i, session := range sr.Sessions {
				sessResult := session.GetSessionResult()
				if sessResult.Plan <= 0 {
					continue
				}
				if _, ok := pBars[i]; !ok {
					bar := uiprogress.AddBar(int(sessResult.Plan)).
						PrependFunc(func(b *uiprogress.Bar) string {
							return fmt.Sprintf("Session:%d %s", i, b.CompletedPercentString())
						}).
						AppendFunc(func(b *uiprogress.Bar) string {
							elapsed := time.Now().Sub(b.TimeStarted)
							return fmt.Sprintf("%d/%d %3dm%02ds", b.Current(), b.Total, int(elapsed.Minutes()), int(elapsed.Seconds())%60)
						})
					bar.TimeStarted = time.Now()
					pBars[i] = bar
				}

				pBars[i].Set(int(sessResult.Executed))
			}
		}

		stmt := true
		for stmt {
			select {
			case err = <-errChan:
				stmt = false
			default:
				updateProgressbar()
			}
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(200)))
		}

		wg.Wait()
		// Final Update, force progress 100%
		for _, bar := range pBars {
			bar.Set(bar.Total)
		}
		time.Sleep(uiprogress.RefreshInterval)
		uiprogress.Stop()
	}

	// error handle and fallback to SimpleUpdate
	if err != nil {
		e := reflect.ValueOf(err).Elem()
		switch {
		case e.Type() == reflect.TypeOf(splmysql.NoUsableColumnError{}):
			if fallback {
				logger.Warnf("No splittable column. Fallback to simple update.\n")

				_, err := sr.SimpleUpdate(sql)
				if err != nil {
					return cli.NewExitError(err.Error(), 1)
				}
				return nil
			}
			e2 := e.Interface().(splmysql.NoUsableColumnError)
			return cli.NewExitError(e2.Error(), e2.Code())

		case e.Type() == reflect.TypeOf(splmysql.InvalidUpdateQueryError{}):
			if fallback {
				logger.Warnf("No splittable column. Fallback to simple update.\n")

				_, err := sr.SimpleUpdate(sql)
				if err != nil {
					return cli.NewExitError(err.Error(), 1)
				}
				return nil
			}
			e2 := e.Interface().(splmysql.InvalidUpdateQueryError)
			return cli.NewExitError(e2.Error(), e2.Code())

		default:
			return cli.NewExitError(err.Error(), 1)
		}
	}

	// Create result and print
	totalResult := splmysql.NewResult(0)
	firstPlanned := int64(0)
	finallyFailed := int64(0)
	for n, sess := range sr.Sessions {
		sessResult := sess.GetSessionResult()
		totalResult.Append(sessResult)
		if n == 0 {
			firstPlanned = sessResult.Plan
		}
		// update with final session result
		finallyFailed = sessResult.Failed
	}

	logger.Debugf("SESSIONS: Planned %d queries and %d executed - %d succeeded / %d failed",
		firstPlanned, totalResult.Executed, totalResult.Succeeded, totalResult.Failed)
	// Output result force
	loglevelBefore := logger.Level
	logger.Level = logrus.InfoLevel
	logger.Infof("RESULT: %d queries affected and %d rows updated. %d queries failed.",
		totalResult.Succeeded, totalResult.RowsAffected, finallyFailed)
	logger.Level = loglevelBefore
	return err
}

func main() {
	cli.VersionFlag = cli.BoolFlag{
		Name:  "version, V",
		Usage: "print only the version.",
	}
	cli.HelpFlag = cli.BoolFlag{
		Name:  "help, ?",
		Usage: "print help message and exit.",
	}

	app := cli.NewApp()
	app.Name = "split_mysql"
	app.Version = Version
	app.Usage = "Split large update transaction query into small transaction queries."
	app.UsageText = fmt.Sprintf("%s [-c CONF|-h HOST -u USER -p PASSWD] -D DATABASE -e QUERY", app.Name)
	app.Author = "etsxxx"
	app.Flags = globalFlags
	app.Action = doMain

	app.Run(os.Args)
}
