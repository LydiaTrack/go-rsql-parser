package go_rsql_parser

import (
	"errors"
	"go.mongodb.org/mongo-driver/bson"
	"strings"
)

const (
	MongoDB = "mongo"
)

// validOperators is a list of valid RSQL operators.
var validOperators = [11]string{"==", "eq", "ne", "gt", "ge", "lt", "le", "in", "out", "like", "ilike"}

type QueryPart struct {
	Field    string
	Operator string
	Value    string
}

// ParseRSQL parses the given RSQL query string and returns the parsed query.
func ParseRSQL(query string, dbType string) (bson.M, error) {
	if dbType == MongoDB {
		// Split the query into parts
		parts := splitQuery(query)
		// Convert the parts to a MongoDB query
		mongoQuery, err := convertToMongoQuery(parts)
		if err != nil {
			return nil, err
		}
		return mongoQuery, nil
	} else {
		return nil, errors.New("unsupported database type")
	}
}

// splitQuery splits the given RSQL query string into parts.
func splitQuery(fullQuery string) []QueryPart {
	// Split the query into parts
	queryParts := strings.Split(fullQuery, ";")
	// Each part in the query is a field, operator, and value separated by "==", and can be in the form of "field==value" or "field=={operator}=value"
	parts := make([]QueryPart, 0)
	for _, part := range queryParts {
		// Split the part into field, operator, and value
		partParts := strings.Split(part, "==")
		if len(partParts) == 2 {
			parts = append(parts, QueryPart{Field: partParts[0], Operator: "==", Value: partParts[1]})
		} else if len(partParts) == 3 {
			parts = append(parts, QueryPart{Field: partParts[0], Operator: partParts[1], Value: partParts[2]})
		}
	}
	return parts
}

// convertToMongoQuery converts the given RSQL query parts to a MongoDB query.
func convertToMongoQuery(parts []QueryPart) (bson.M, error) {
	var query bson.M = bson.M{}
	for _, part := range parts {
		if !isValidOperator(part.Operator) {
			return nil, errors.New("invalid operator: " + part.Operator)
		} else {
			// Convert the part to a MongoDB query
			if part.Operator == "eq" || part.Operator == "==" {
				query[part.Field] = bson.M{"$eq": part.Value}
			} else if part.Operator == "ne" {
				query[part.Field] = bson.M{"$ne": part.Value}
			} else if part.Operator == "gt" {
				query[part.Field] = bson.M{"$gt": part.Value}
			} else if part.Operator == "ge" {
				query[part.Field] = bson.M{"$gte": part.Value}
			} else if part.Operator == "lt" {
				query[part.Field] = bson.M{"$lt": part.Value}
			} else if part.Operator == "le" {
				query[part.Field] = bson.M{"$lte": part.Value}
			} else if part.Operator == "in" {
				// Value will be in the form "(value1,value2,value3)", so we ignore the first and last parentheses and split the values by ","
				values := strings.Split(part.Value[1:len(part.Value)-1], ",")
				query[part.Field] = bson.M{"$in": values}
			} else if part.Operator == "out" {
				// Value will be in the form "(value1,value2,value3)", so we ignore the first and last parentheses and split the values by ","
				values := strings.Split(part.Value[1:len(part.Value)-1], ",")
				query[part.Field] = bson.M{"$nin": values}
			} else if part.Operator == "like" {
				query[part.Field] = bson.M{"$regex": part.Value}
			} else if part.Operator == "ilike" {
				query[part.Field] = bson.M{"$regex": "(?i)" + part.Value}
			}
		}
	}
	return query, nil
}

// isValidOperator checks if the given operator is a valid RSQL operator.
func isValidOperator(operator string) bool {
	for _, validOperator := range validOperators {
		if operator == validOperator {
			return true
		}
	}
	return false
}
