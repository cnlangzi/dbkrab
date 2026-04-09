package sql

import (
	"database/sql"
	"regexp"
	"strings"
)

// Parameter represents a named parameter in SQL template
type Parameter struct {
	Name  string // @orders_order_id
	Value interface{}
}

// ParamExtractor extracts @name parameters from SQL templates
// and converts them to driver-specific parameter formats
type ParamExtractor struct {
	pattern *regexp.Regexp
}

// NewParamExtractor creates a new parameter extractor
func NewParamExtractor() *ParamExtractor {
	return &ParamExtractor{
		pattern: regexp.MustCompile(`@([a-zA-Z_][a-zA-Z0-9_]*)`),
	}
}

// ExtractParamNames extracts all parameter names from SQL template in order
func (e *ParamExtractor) ExtractParamNames(sqlTmpl string) []string {
	matches := e.pattern.FindAllStringSubmatch(sqlTmpl, -1)
	names := make([]string, 0, len(matches))
	seen := make(map[string]bool)

	for _, match := range matches {
		if len(match) >= 2 {
			name := strings.ToLower(match[1])
			if !seen[name] {
				names = append(names, name)
				seen[name] = true
			}
		}
	}
	return names
}

// ExtractOption defines how to convert @name parameters
type ExtractOption int

const (
	// OptionPlaceholders converts @name to ? (for SQLite)
	OptionPlaceholders ExtractOption = iota
	// OptionKeepNames keeps @name (for MSSQL)
	OptionKeepNames
)

// Extract converts SQL template to driver-specific format
// sqlTmpl: SELECT * FROM orders WHERE order_id = @orders_order_id
// params: map of parameter name → value
// Returns: converted SQL, args/named args, error
func (e *ParamExtractor) Extract(sqlTmpl string, params map[string]interface{}, opt ExtractOption) (string, []interface{}, error) {
	names := e.ExtractParamNames(sqlTmpl)

	switch opt {
	case OptionPlaceholders:
		// SQLite: @name → ?
		args := make([]interface{}, len(names))
		for i, name := range names {
			args[i] = params[name]
		}
		sql := e.pattern.ReplaceAllString(sqlTmpl, "?")
		return sql, args, nil

	case OptionKeepNames:
		// MSSQL: keep @name, use named args
		args := make([]interface{}, len(names))
		for i, name := range names {
			args[i] = sql.NamedArg{Name: name, Value: params[name]}
		}
		return sqlTmpl, args, nil

	default:
		return e.Extract(sqlTmpl, params, OptionPlaceholders)
	}
}

// ExtractNamed extracts parameters for MSSQL named args
func (e *ParamExtractor) ExtractNamed(sqlTmpl string, params map[string]interface{}) (string, []interface{}, error) {
	names := e.ExtractParamNames(sqlTmpl)

	namedArgs := make([]interface{}, len(names))
	for i, name := range names {
		namedArgs[i] = sql.NamedArg{Name: name, Value: params[name]}
	}

	return sqlTmpl, namedArgs, nil
}

// ReplacePlaceholders replaces @name with ? in SQL template
func (e *ParamExtractor) ReplacePlaceholders(sqlTmpl string) string {
	return e.pattern.ReplaceAllString(sqlTmpl, "?")
}

// Validate checks if all @name parameters in SQL have corresponding values
func (e *ParamExtractor) Validate(sqlTmpl string, params map[string]interface{}) error {
	names := e.ExtractParamNames(sqlTmpl)
	for _, name := range names {
		if _, ok := params[name]; !ok {
			return ErrMissingParameter
		}
	}
	return nil
}
