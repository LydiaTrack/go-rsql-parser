package go_rsql_parser

import (
	"go.mongodb.org/mongo-driver/bson"
	"strconv"
	"testing"
)

// compareSlice compares two slice of strings and returns true if they are equal, false otherwise.
func compareSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, value := range a {
		if value != b[i] {
			return false
		}
	}
	return true
}

// compareMaps compares two maps and returns true if they are equal, false otherwise.
func compareMaps(a, b bson.M) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		// value is a map, so compare recursively
		if _, ok := value.(bson.M); ok {
			if !compareMaps(value.(bson.M), b[key].(bson.M)) {
				return false
			}
		} else {
			// If both values are strings, compare them. Otherwise, convert them to strings and compare.
			bsonValue := b[key]
			// If the value or bsonValue is an int, float, etc., convert it to a string for comparison
			if _, ok := value.(int); ok {
				value = strconv.Itoa(value.(int))
			}
			if _, ok := bsonValue.(int); ok {
				bsonValue = strconv.Itoa(bsonValue.(int))
			}
			if _, ok := value.(float64); ok {
				value = strconv.FormatFloat(value.(float64), 'f', -1, 64)
			}
			if _, ok := bsonValue.(float64); ok {
				bsonValue = strconv.FormatFloat(bsonValue.(float64), 'f', -1, 64)
			}
			if _, ok := value.(bool); ok {
				value = strconv.FormatBool(value.(bool))
			}
			if _, ok := bsonValue.(bool); ok {
				bsonValue = strconv.FormatBool(bsonValue.(bool))
			}

			// If both values are slices, then compare them with reflect.DeepEqual
			if _, ok := value.([]string); ok {
				if _, ok := bsonValue.([]string); ok {
					return compareSlice(value.([]string), bsonValue.([]string))
				}
			}

			if value != bsonValue {
				return false
			}
		}
	}
	return true
}

// TestRSQLParser gathers all the tests for the RSQL parser.
func TestRSQLParser(t *testing.T) {
	t.Run("TestParseRSQLUnsupportedDBType", testParseRSQLUnsupportedDBType)
	t.Run("TestParseWithInvalidOperator", testParseWithInvalidOperator)
	t.Run("TestParseRSQLMultiple", testParseRSQLMultiple)
	t.Run("TestParseRSQLEqualsSingle", testParseRSQLEqualsSingle)
	t.Run("TestParseRSQLSingleGreaterThan", testParseRSQLSingleGreaterThan)
	t.Run("TestParseRSQLSingleGreaterThanOrEqual", testParseRSQLSingleGreaterThanOrEqual)
	t.Run("TestParseRSQLSingleLessThan", testParseRSQLSingleLessThan)
	t.Run("TestParseRSQLSingleLessThanOrEqual", testParseRSQLSingleLessThanOrEqual)
	t.Run("TestParseRSQLSingleNotEqual", testParseRSQLSingleNotEqual)
	t.Run("TestParseRSQLSingleIn", testParseRSQLSingleIn)
	t.Run("TestParseRSQLSingleNotIn", testParseRSQLSingleNotIn)
	t.Run("TestParseRSQLSingleLike", testParseRSQLSingleLike)
	t.Run("TestParseRSQLSingleILike", testParseRSQLSingleILike)
}

// testParseRSQLUnsupportedDBType tests parsing an RSQL query with an unsupported database type.
func testParseRSQLUnsupportedDBType(t *testing.T) {
	query := "name==John"
	dbType := "MySQL"
	_, err := ParseRSQL(query, dbType)
	if err == nil {
		t.Errorf("Expected an error for an unsupported database type")
	}
}

// testParseWithInvalidOperator tests parsing an RSQL query with an invalid operator.
func testParseWithInvalidOperator(t *testing.T) {
	query := "name==invalid==John"
	dbType := MongoDB
	_, err := ParseRSQL(query, dbType)
	if err == nil {
		t.Errorf("Expected an error for an invalid operator")
	}

	query = "age==eqs==30"
	dbType = MongoDB
	_, err = ParseRSQL(query, dbType)
	if err == nil {
		t.Errorf("Expected an error for an invalid operator")
	}
}

// testParseRSQLUnsupportedDBType tests parsing an RSQL query multiple parts.
func testParseRSQLMultiple(t *testing.T) {
	query := "name==eq==John;age==gt==30;city==like==New York"
	dbType := MongoDB
	parsedQuery, err := ParseRSQL(query, dbType)
	if err != nil {
		t.Errorf("Error parsing RSQL query: %s", err)
	}
	expectedQuery := bson.M{
		"name": bson.M{
			"$eq": "John",
		},
		"age": bson.M{
			"$gt": 30,
		},
		"city": bson.M{
			"$regex": "New York",
		},
	}
	if !compareMaps(parsedQuery, expectedQuery) {
		t.Errorf("Parsed query does not match expected query")
	}
}

// testParseRSQLEqualsSingle tests parsing a single RSQL query with an equality operator (==).
func testParseRSQLEqualsSingle(t *testing.T) {
	query := "name==John"
	dbType := MongoDB
	parsedQuery, err := ParseRSQL(query, dbType)
	if err != nil {
		t.Errorf("Error parsing RSQL query: %s", err)
	}
	expectedQuery := bson.M{
		"name": bson.M{
			"$eq": "John",
		},
	}
	if !compareMaps(parsedQuery, expectedQuery) {
		t.Errorf("Parsed query does not match expected query")
	}

	// Test with an integer value
	query = "age==30"
	parsedQuery, err = ParseRSQL(query, dbType)
	if err != nil {
		t.Errorf("Error parsing RSQL query: %s", err)
	}
	expectedQuery = bson.M{
		"age": bson.M{
			"$eq": 30,
		},
	}
	if !compareMaps(parsedQuery, expectedQuery) {
		t.Errorf("Parsed query does not match expected query")
	}

	// Test with a float value
	query = "price==30.5"
	parsedQuery, err = ParseRSQL(query, dbType)
	if err != nil {
		t.Errorf("Error parsing RSQL query: %s", err)
	}
	expectedQuery = bson.M{
		"price": bson.M{
			"$eq": 30.5,
		},
	}
	if !compareMaps(parsedQuery, expectedQuery) {
		t.Errorf("Parsed query does not match expected query")
	}

	// Test with a boolean value
	query = "active==true"
	parsedQuery, err = ParseRSQL(query, dbType)
	if err != nil {
		t.Errorf("Error parsing RSQL query: %s", err)
	}

	expectedQuery = bson.M{
		"active": bson.M{
			"$eq": true,
		},
	}
	if !compareMaps(parsedQuery, expectedQuery) {
		t.Errorf("Parsed query does not match expected query")
	}
}

// testParseRSQLSingleGreaterThan tests parsing a single RSQL query with a greater than operator (>).
func testParseRSQLSingleGreaterThan(t *testing.T) {
	query := "age==gt==30"
	dbType := MongoDB
	parsedQuery, err := ParseRSQL(query, dbType)
	if err != nil {
		t.Errorf("Error parsing RSQL query: %s", err)
	}
	expectedQuery := bson.M{
		"age": bson.M{
			"$gt": 30,
		},
	}
	if !compareMaps(parsedQuery, expectedQuery) {
		t.Errorf("Parsed query does not match expected query")
	}

	// Test with a float value
	query = "price==gt==30.5"
	parsedQuery, err = ParseRSQL(query, dbType)
	if err != nil {
		t.Errorf("Error parsing RSQL query: %s", err)
	}
}

// testParseRSQLSingleGreaterThanOrEqual tests parsing a single RSQL query with a greater than or equal operator (>=).
func testParseRSQLSingleGreaterThanOrEqual(t *testing.T) {
	query := "age==ge==30"
	dbType := MongoDB
	parsedQuery, err := ParseRSQL(query, dbType)
	if err != nil {
		t.Errorf("Error parsing RSQL query: %s", err)
	}
	expectedQuery := bson.M{
		"age": bson.M{
			"$gte": 30,
		},
	}
	if !compareMaps(parsedQuery, expectedQuery) {
		t.Errorf("Parsed query does not match expected query")
	}

	// Test with a float value
	query = "price==ge==30.5"
	parsedQuery, err = ParseRSQL(query, dbType)
	if err != nil {
		t.Errorf("Error parsing RSQL query: %s", err)
	}
}

// testParseRSQLSingleLessThan tests parsing a single RSQL query with a less than operator (<).
func testParseRSQLSingleLessThan(t *testing.T) {
	query := "age==lt==30"
	dbType := MongoDB
	parsedQuery, err := ParseRSQL(query, dbType)
	if err != nil {
		t.Errorf("Error parsing RSQL query: %s", err)
	}
	expectedQuery := bson.M{
		"age": bson.M{
			"$lt": 30,
		},
	}
	if !compareMaps(parsedQuery, expectedQuery) {
		t.Errorf("Parsed query does not match expected query")
	}

	// Test with a float value
	query = "price==lt==30.5"
	parsedQuery, err = ParseRSQL(query, dbType)
	if err != nil {
		t.Errorf("Error parsing RSQL query: %s", err)
	}
}

// testParseRSQLSingleLessThanOrEqual tests parsing a single RSQL query with a less than or equal operator (<=).
func testParseRSQLSingleLessThanOrEqual(t *testing.T) {
	query := "age==le==30"
	dbType := MongoDB
	parsedQuery, err := ParseRSQL(query, dbType)
	if err != nil {
		t.Errorf("Error parsing RSQL query: %s", err)
	}
	expectedQuery := bson.M{
		"age": bson.M{
			"$lte": 30,
		},
	}
	if !compareMaps(parsedQuery, expectedQuery) {
		t.Errorf("Parsed query does not match expected query")
	}

	// Test with a float value
	query = "price==le==30.5"
	parsedQuery, err = ParseRSQL(query, dbType)
	if err != nil {
		t.Errorf("Error parsing RSQL query: %s", err)
	}
}

// testParseRSQLSingleNotEqual tests parsing a single RSQL query with a not equal operator (!=).
func testParseRSQLSingleNotEqual(t *testing.T) {
	query := "name==ne==John"
	dbType := MongoDB
	parsedQuery, err := ParseRSQL(query, dbType)
	if err != nil {
		t.Errorf("Error parsing RSQL query: %s", err)
	}
	expectedQuery := bson.M{
		"name": bson.M{
			"$ne": "John",
		},
	}
	if !compareMaps(parsedQuery, expectedQuery) {
		t.Errorf("Parsed query does not match expected query")
	}
}

// testParseRSQLSingleIn tests parsing a single RSQL query with an in operator (=in=).
func testParseRSQLSingleIn(t *testing.T) {
	query := "name==in==(John,Jane,Doe)"
	dbType := MongoDB
	parsedQuery, err := ParseRSQL(query, dbType)
	if err != nil {
		t.Errorf("Error parsing RSQL query: %s", err)
	}
	expectedQuery := bson.M{
		"name": bson.M{
			"$in": []string{"John", "Jane", "Doe"},
		},
	}
	if !compareMaps(parsedQuery, expectedQuery) {
		t.Errorf("Parsed query does not match expected query")
	}
}

// testParseRSQLSingleNotIn tests parsing a single RSQL query with a not in operator (=out=).
func testParseRSQLSingleNotIn(t *testing.T) {
	query := "name==out==(John,Jane,Doe)"
	dbType := MongoDB
	parsedQuery, err := ParseRSQL(query, dbType)
	if err != nil {
		t.Errorf("Error parsing RSQL query: %s", err)
	}
	expectedQuery := bson.M{
		"name": bson.M{
			"$nin": []string{"John", "Jane", "Doe"},
		},
	}
	if !compareMaps(parsedQuery, expectedQuery) {
		t.Errorf("Parsed query does not match expected query")
	}

}

// testParseRSQLSingleLike tests parsing a single RSQL query with a like operator (=like=).
func testParseRSQLSingleLike(t *testing.T) {
	query := "name==like==John"
	dbType := MongoDB
	parsedQuery, err := ParseRSQL(query, dbType)
	if err != nil {
		t.Errorf("Error parsing RSQL query: %s", err)
	}
	expectedQuery := bson.M{
		"name": bson.M{
			"$regex": "John",
		},
	}
	if !compareMaps(parsedQuery, expectedQuery) {
		t.Errorf("Parsed query does not match expected query")
	}
}

// testParseRSQLSingleILike tests parsing a single RSQL query with a case-insensitive like operator (=ilike=).
func testParseRSQLSingleILike(t *testing.T) {
	query := "name==ilike==John"
	dbType := MongoDB
	parsedQuery, err := ParseRSQL(query, dbType)
	if err != nil {
		t.Errorf("Error parsing RSQL query: %s", err)
	}
	expectedQuery := bson.M{
		"name": bson.M{
			"$regex": "(?i)John",
		},
	}
	if !compareMaps(parsedQuery, expectedQuery) {
		t.Errorf("Parsed query does not match expected query")
	}
}
