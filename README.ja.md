# split_mysql

MySQLの単一の更新クエリを分割して、複数の小さなトランザクションにするCLIツールです。

## これはなに？

MySQLを運用していてこんな経験はありませんか？

- 数千万行あるようなテーブルに大規模なUPDATE文を流したことはありませんか？
- そのせいでテーブル全体にロックが発生したことはありませんか？
- しかも何分待ってもUPDATEが終わらなかったことはありませんか？
- 強制終了したらロックとロールバックでより酷いことになったことはありませんか？

私にはあります。

「大きなトランザクションの単一クエリを分割して、小さなトランザクションの複数クエリにする」

`split_mysql`はその自動化ツールです。

## 使い方

このような`mysql`コマンドを実行しているとします。

```bash:before
mysql -D theDB -e "UPDATE theTable SET ... WHERE foo = 'bar';"
```

このように`mysql`を`split_mysql`に置き換えましょう。

UPDATE文は自動分割されて実行されます。

```bash:after
### dryrun
split_mysql -D theDB -e "UPDATE theTable SET ... WHERE foo = 'bar';" -n

### execute
split_mysql -D theDB -e "UPDATE theTable SET ... WHERE foo = 'bar';"
```

`--parallel`オプションにより、分割後のクエリを並列実行することもできます。

```bash:parallel example
### execute parallel
split_mysql -D theDB -e "UPDATE theTable SET ... WHERE foo = 'bar';" --parallel 8
```

その他のオプションについて、詳しくは`--help`を参照してください

## インストールとビルド

`go get`を使用します。

```bash
go get github.com/etsxxx/split_mysql
```

依存関係は[Glide](https://github.com/Masterminds/glide)で管理しています。
`glide install`で依存パッケージをインストールし、ビルドしましょう。

```bash
glide install
go build
```

## 仕組み

`split_mysql`は、UPDATE対象のテーブルから「分割可能なカラム」を検索し、
`WHERE ... BETWEEN`句で小さなサイズに分割したUPDATE文を生成し実行します。

現在の実装では以下を「分割可能なカラム」として検索します。

- 整数型 + NOT NULL制約 + 以下のいずれか。
  - Primary Key
  - Unique Key
  - AUTO_INCREMENT

条件を満たすカラムが存在しないテーブルには実行できません。
（その場合、`--fallback`オプションを付与しているとオリジナルのUPDATE文を実行します)

## メリット・デメリット

**デメリットを必ず読んで下さい。**

### メリット

- トランザクションが小さくなります。
  - 強制終了した場合に大きなロックやROLLBACKが発生しにくくなります。
  - Galera Cluster(Percona XtraDB Cluster)の制限に当たりにくくなります。
  - ロックの衝突可能性が減ります。
- `--parallel`オプションの並列実行により、環境によってはUPDATEが速くなる可能性があります。
  - 4coreマシンの検証環境では倍速になりました。

### デメリット

- **更新全体で単一のトランザクションにはなりません。**
  - 強制終了すると、部分的なロールバックが走ります。
  - 一部クエリがエラーとなった場合も部分的なロールバックが走ります。
  - 失敗した行を見つけられない場合、再度更新することは困難でしょう。
- 大量のUPDATE文が流れるため、監査ログが汚れます。

## ライセンス

[LICENSE](LICENSE)を参照してください。