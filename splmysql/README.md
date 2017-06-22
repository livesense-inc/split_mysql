# splmysql

(This is beta)
MySQL wrapper library to split single UPDATE query into many tiny transaction queries. 


## How to use

### Create Runner instance
Create runner instance with connection information.

```golang
sr, err := splmysql.NewByOptions("DB Name", "HostName", 3306, "USER", "PASSWORD", "CharacterSet")
if err != nil {
    return err
}
defer sr.Close()

```

You can use my.cnf.

```golang
sr, err := splmysql.NewByConf("DB Name", "path-to-my.cnf")
if err != nil {
    return err
}
defer sr.Close()
```

`NewByOptions` and `NewByConf` do not establish any connections to the database.
They create connection information only.

### Execute query

Create session object to get table information and test connection parameters.
It's ok, execute query.

```golang
// Create session. Runner connect to DB and get table information.
// If connection information has invalid parameters, error returns. 
sessionData, err := sr.NewSession(sql)
if err != nil {
    return err
}

// Let's execute parallel
retrySessionData, err := sr.RunParallel(sessionData, numberOfParallel)
```

`RunParallel()` returns Session object `retrySessionData` to retry failed queries.

### Fallback

If `NewSession()` returns `NoUsableColumnError`, you can run `SimpleUpdate()` as fallback.
This method does not split query. Be careful to use Galera Cluster environment.

```golang
sess, err := sr.NewSession(sql)
if err != nil {
	e := reflect.ValueOf(err).Elem()
	switch {
	case e.Type() == reflect.TypeOf(splmysql.NoUsableColumnError{}):
        _, err := sr.SimpleUpdate(sql)
        if err != nil {
            return err
        }
    default:
        return err
    }
}
```


### Logging

`splmysql` is using [Logrus](https://github.com/sirupsen/logrus) logger.
You can overide it your own Logrus logger before call `NewSession()`.

```golang
logger := logrus.New()
logger.Formatter = &logrus.TextFormatter{
    FullTimestamp: true,
}
logger.Out = os.Stdout

// Overide Logger
sr.Logger = logger
```
