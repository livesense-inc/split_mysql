package splmysql

/*
Utility functions which not belongs to splmysql.
*/

import (
	"fmt"
	"math/rand"
	"regexp"
	"strings"
)

func sqlIncludesWhere(sql string) bool {
	regexWhere := regexp.MustCompile(`\bwhere\b`)
	if regexWhere.FindStringIndex(strings.ToLower(sql)) != nil {
		return true
	}
	return false
}

func isUpdateQuery(sql string) bool {
	re := regexp.MustCompile(`^\s*update\s+.+\s+set\s.+$`)
	return re.MatchString(strings.ToLower(sql))
}

func isLimitedQuery(sql string) bool {
	re := regexp.MustCompile(`^\s*.+limit\s+[0-9]+;?$`)
	return re.MatchString(strings.ToLower(sql))
}

func getUpdateTableName(sql string) string {
	re := regexp.MustCompile(`^\s*update\s+(.+)\s+set\s.+$`)
	s := re.ReplaceAllString(strings.ToLower(sql), "$1")
	if strings.Index(s, ",") < 0 {
		return strings.Trim(s, " ")
	}
	return ""
}

func getSplittedUpdateSQL(originalSQL string, splitColumnName string, start int64, end int64) string {
	if sqlIncludesWhere(originalSQL) {
		return fmt.Sprintf("%s and %s between %d and %d", originalSQL, splitColumnName, start, end)
	}
	return fmt.Sprintf("%s where %s between %d and %d", originalSQL, splitColumnName, start, end)
}

func shuffleTransactions(transactions []*Transaction) {
	n := len(transactions)
	for i := n - 1; i >= 0; i-- {
		j := rand.Intn(i + 1)
		transactions[i], transactions[j] = transactions[j], transactions[i]
	}
}

func isIntegerType(typeName string) bool {
	typeName = strings.ToLower(strings.Trim(typeName, " "))
	if strings.Index(typeName, "int") >= 0 {
		return true
	}
	return false
}

// findColumnNameForSplit parses 'SHOW CREATE TABLE' info and get column name for split
func findColumnNameForSplit(info string) (columnName string) {
	if columnName = parseSinglePrimaryKeyInfo(info); columnName != "" {
		return columnName
	} else if columnName = parseSingleUniqueKeyInfo(info); columnName != "" {
		return columnName
	} else if columnName = parseAutoIncrementInfo(info); columnName != "" {
		return columnName
	}
	return ""
}

// parseSinglePrimaryKeyInfo parses single-column Primary Key info.
// a.k.a. multi-columns Primary Key is invalid in this func.
func parseSinglePrimaryKeyInfo(info string) (columnName string) {
	var pkColumnName string
	//	PRIMARY KEY (`pk`),\n
	rePKInfo := regexp.MustCompile(
		`^.*\sprimary\s+key` + // PRIMARY KEY
			`\s*\([` + "`" + `'"]([^` + "`" + `'"]+)[` + "`" + `'"]\).*$`) // (`pk`),\n
	for _, line := range strings.Split(info, "\n") {
		line = strings.ToLower(line)
		if !rePKInfo.MatchString(line) {
			continue
		}
		pkColumnName = rePKInfo.ReplaceAllString(line, "$1")
		break
	}
	if pkColumnName == "" {
		return ""
	}

	// `pk` int(10) unsigned NOT NULL,\n
	reColumn := regexp.MustCompile(
		`^.*\s[` + "`" + `'"]([^` + "`" + `'"]+)[` + "`" + `'"]` + // `pk`
			`\s+([^\s]+)(\s.*)?` + // `int(10) unsigned`
			`\snot\s+null.*$`) // NOT NULL
	for _, line := range strings.Split(info, "\n") {
		line = strings.ToLower(line)
		if !reColumn.MatchString(line) {
			continue
		}
		columnName = reColumn.ReplaceAllString(line, "$1")
		columnType := reColumn.ReplaceAllString(line, "$2")
		if pkColumnName == columnName && isIntegerType(columnType) {
			return columnName
		}
	}

	return ""
}

// parseSingleUniqueKeyInfo parses single-column Unique Key with 'NOT NULL' statement.
// a.k.a. multi-columns Unique Key is invalid in this func.
func parseSingleUniqueKeyInfo(info string) (columnName string) {
	var ukColumnNames []string
	// UNIQUE KEY `unique_key` (`uk`),\n
	reUKInfo := regexp.MustCompile(
		`^.*\sunique\s+key\s+` + // UNIQUE KEY
			`[` + "`" + `'"][^` + "`" + `'"]+[` + "`" + `'"]` + // `unique_key`
			`\s*\([` + "`" + `'"]([^` + "`" + `'"]+)[` + "`" + `'"]\).*$`) // (`uk`),\n
	for _, line := range strings.Split(info, "\n") {
		line = strings.ToLower(line)
		if !reUKInfo.MatchString(line) {
			continue
		}
		ukColumnNames = append(ukColumnNames, reUKInfo.ReplaceAllString(line, "$1"))
		break
	}
	if len(ukColumnNames) <= 0 {
		return ""
	}

	// `uk` bigint unsigned NOT NULL,\n
	reColumn := regexp.MustCompile(
		`^.*\s[` + "`" + `'"]([^` + "`" + `'"]+)[` + "`" + `'"]` + // `uk`
			`\s+([^\s]+)(\s.*)?\snot\s+null.*$`) // bigint unsigned NOT NULL
	for _, line := range strings.Split(info, "\n") {
		line = strings.ToLower(line)
		if !reColumn.MatchString(line) {
			continue
		}
		columnName = reColumn.ReplaceAllString(line, "$1")
		columnType := reColumn.ReplaceAllString(line, "$2")
		if !isIntegerType(columnType) {
			continue
		}
		for _, s := range ukColumnNames {
			if columnName == s {
				return columnName
			}
		}
	}

	return ""
}

// parseAutoIncrementInfo parses AUTO_INCREMENT column info.
// if AUTO_INCREMENT column does not have 'NOT NULL' statement, it's invalid.
func parseAutoIncrementInfo(info string) (columnName string) {
	// `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n
	re := regexp.MustCompile(
		`^.*\s[` + "`" + `'"]([^` + "`" + `'"]+)[` + "`" + `'"]` + // `id`
			`\s.*not\s+null(\s.*)?\sauto_increment[\s,].*$`) // bigint(20) unsigned NOT NULL AUTO_INCREMENT,\n
	for _, line := range strings.Split(info, "\n") {
		line = strings.ToLower(line)
		if re.MatchString(line) {
			columnName = re.ReplaceAllString(line, "$1")
			return columnName
		}
	}

	return ""
}
