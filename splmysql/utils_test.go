package splmysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsUpdateQuery(t *testing.T) {
	var q string
	q = "UPDATE foo SET yo = 'hey';"
	assert.True(t, isUpdateQuery(q))

	q = "UPDATE foo SET yo = 'hey' WHERE hey = 'yo';"
	assert.True(t, isUpdateQuery(q))

	q = "update foo set yo = 'hey', hey = 'yo';"
	assert.True(t, isUpdateQuery(q))

	q = "INSERT INTO update (yo, hey) VALUES ('hey', 'yo');"
	assert.False(t, isUpdateQuery(q))

	q = "UPDATE foo,(select concat('a', 'b', substring(reverse(rand()), 1, 4)) as col, id from foo) as tmp SET foo.col=tmp.col WHERE foo.col IS NOT NULL and foo.id = tmp.id;"
	assert.True(t, isUpdateQuery(q))

}
func TestIsLimitedQuery(t *testing.T) {
	var q string
	q = "UPDATE foo SET yo = 'hey' LIMIT 100;"
	assert.True(t, isLimitedQuery(q))

	q = "UPDATE foo SET yo = 'hey' WHERE hey = 'yo' limit 10;"
	assert.True(t, isLimitedQuery(q))

	q = "update foo set yo = 'limit', hey = 'yo';"
	assert.False(t, isLimitedQuery(q))
}

func TestGetUpdateTableName(t *testing.T) {
	var q string
	q = "UPDATE foo SET yo = 'hey';"
	assert.Equal(t, getUpdateTableName(q), "foo")

	q = "UPDATE items, month SET items.price=month.price WHERE items.id=month.id;"
	assert.Equal(t, getUpdateTableName(q), "")
}

func TestIsIntegerType(t *testing.T) {
	assert.True(t, isIntegerType("TINYINT"))
	assert.True(t, isIntegerType("TiNyInT"))
	assert.True(t, isIntegerType("  TINYINT  "))
	assert.True(t, isIntegerType("TiNyInT"))
	assert.True(t, isIntegerType("tinyint"))
	assert.True(t, isIntegerType("smallint"))
	assert.True(t, isIntegerType("mediumint"))
	assert.True(t, isIntegerType("int"))
	assert.True(t, isIntegerType("bigint"))

	assert.False(t, isIntegerType("varchar"))
}

func TestFindColumnNameForSplit(t *testing.T) {
	// Primary Key and type is integer-like.
	stubInfo := "CREATE TABLE `sent` (\n" +
		"  `pk` int(10) unsigned NOT NULL,\n" +
		"  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n" +
		"  `uk` varchar(50) NOT NULL,\n" +
		"  `time` datetime NOT NULL,\n" +
		"  PRIMARY KEY (`pk`),\n" +
		"  UNIQUE KEY `unique_key` (`uk`),\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8"
	columnName := findColumnNameForSplit(stubInfo)
	assert.Equal(t, columnName, "pk")

	// Primary Key and type is integer-like.
	stubInfo = "CREATE TABLE `sent` (\n" +
		"  `pk` bigint(20) unsigned NOT NULL,\n" +
		"  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n" +
		"  `uk` varchar(50) NOT NULL,\n" +
		"  `time` datetime NOT NULL,\n" +
		"  PRIMARY KEY (`pk`),\n" +
		"  UNIQUE KEY `unique_key` (`uk`),\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8"
	columnName = findColumnNameForSplit(stubInfo)
	assert.Equal(t, columnName, "pk")

	// Primary Key and type is integer-like.
	stubInfo = "CREATE TABLE `sent` (\n" +
		"  `pk` tinyint unsigned NOT NULL,\n" +
		"  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n" +
		"  `uk` varchar(50) NOT NULL,\n" +
		"  `time` datetime NOT NULL,\n" +
		"  PRIMARY KEY (`pk`),\n" +
		"  UNIQUE KEY `unique_key` (`uk`),\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8"
	columnName = findColumnNameForSplit(stubInfo)
	assert.Equal(t, columnName, "pk")

	// Unique Key with NOT NULL
	stubInfo = "CREATE TABLE `sent` (\n" +
		"  `pk` varchar(10) NOT NULL,\n" +
		"  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n" +
		"  `uk` bigint unsigned NOT NULL,\n" +
		"  `time` datetime NOT NULL,\n" +
		"  PRIMARY KEY (`pk`),\n" +
		"  UNIQUE KEY `unique_key` (`uk`),\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8"
	columnName = findColumnNameForSplit(stubInfo)
	assert.Equal(t, columnName, "uk")

	// AUTO_INCREMENT with NOT NULL
	stubInfo = "CREATE TABLE `sent` (\n" +
		"  `pk` varchar(10) NOT NULL,\n" +
		"  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n" +
		"  `uk` varchar(50) NOT NULL,\n" +
		"  `time` datetime NOT NULL,\n" +
		"  PRIMARY KEY (`pk`),\n" +
		"  UNIQUE KEY `unique_key` (`uk`),\n" +
		") ENGINE=InnoDB AUTO_INCREMENT=4371071214 DEFAULT CHARSET=utf8"

	columnName = findColumnNameForSplit(stubInfo)
	assert.Equal(t, columnName, "id")

	// AUTO_INCREMENT with NOT NULL
	stubInfo = "CREATE TABLE `sent` (\n" +
		"  `pk` varchar(10) NOT NULL,\n" +
		"  `id` bigint(20) unsigned NOT NULL,\n" +
		"  `uk` varchar(50) NOT NULL,\n" +
		"  `time` datetime NOT NULL,\n" +
		"  PRIMARY KEY (`pk`),\n" +
		"  UNIQUE KEY `unique_key` (`uk`),\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8"

	columnName = findColumnNameForSplit(stubInfo)
	assert.Equal(t, columnName, "")

}
