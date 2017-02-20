# split_mysql

(This is beta)
MySQL CLI tool to split single UPDATE query into many tiny transaction queries. 

## What is this

Have you ever been experienced like following:

- Someone executed large UPDATE query into the table which has thousands millions rows.
- UPDATE produced the table lock.
- UPDATE did not end even after waiting for many minutes.
- When you killed it, it produced more large LOCK and ROLLBACK. 

Ya, I experienced.

"Split single query with large transaction, create multi queries with tiny transactions."

`split_mysql` is automation tool for it.

## Usage

Before: `mysql` command like this.

```bash:before
mysql -D theDB -e "UPDATE theTable SET ... WHERE foo = 'bar';"
```

After: replace `mysql` with `split_mysql`.

UPDATE will be splitted automatically.

```bash:after
### dryrun
split_mysql -D theDB -e "UPDATE theTable SET ... WHERE foo = 'bar';" -n

### execute
split_mysql -D theDB -e "UPDATE theTable SET ... WHERE foo = 'bar';"
```

`--parallel` option creates concurrent executions.

```bash:parallel example
### execute parallel
split_mysql -D theDB -e "UPDATE theTable SET ... WHERE foo = 'bar';" --parallel 8
```

More options, see `--help`.

## Install and Build

Use `go get`

```bash
go get github.com/etsxxx/split_mysql
```

Dependencies are managed by [Glide](https://github.com/Masterminds/glide).
Execute `glide install` and build.

```bash
glide install
go build
```

## How it works

`split_mysql` finds a 'splittable column' from the table, 
create new splitted UPDATE queries with `WHERE ... BETWEEN` and execute.

Current implementation, 'splittable column' condition is:

- Integer type with NOT NULL constraint + one of following conditions:
  - Primary Key
  - Unique Key
  - AUTO_INCREMENT

If the table not have the 'splittable column', `split_mysql` fails.
（But original UPDATE query will execute with `--fallback` option.)

## Pros / Cons

**You MUST read Cons!**

### Pros

- Small transactions.
  - If you killed it, LOCK and ROLLBACK will be smaller also.
  - Reducing the possibility of hitting the limits of Galera Cluster (Percona XtraDB Cluster).
  - Reducing the possibility of dead locks.
- Faster UPDATE with `--parallel` option in some conditions.
  - My test environment, 4 core machine, 2x faster.

### Cons

- **Single transaction never works entire UPDATE queries.**
  - If you kill it, partial ROLLBACKs occur.
  - If some queries failed, partial ROLLBACKs occur.
  - If you cannot find failed columns, it difficult to UPDATE them. 
- The many UPDATE queries make dirty audit log.

## License

See [LICENSE](LICENSE) for details.