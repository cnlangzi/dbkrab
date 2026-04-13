package sql

import (
	"fmt"
	"strings"

	"github.com/ha1tch/tsqlparser"
	"github.com/ha1tch/tsqlparser/ast"
)

// extractOutputFields extracts output field names from a SQL SELECT statement.
// It follows these rules:
// - If a column has an AS alias, use the alias
// - If no alias, strip any table prefix (e.g. o.order_id -> order_id) and use the column name
// - @variable tokens (e.g. @cdc_lsn) map to the variable name without the @ (e.g. cdc_lsn)
// Returns an error if tsqlparser fails to parse the SQL.
func extractOutputFields(sql string) ([]string, error) {
	program, parseErrors := tsqlparser.Parse(sql)
	if len(parseErrors) > 0 {
		return nil, fmt.Errorf("parse errors: %v", parseErrors)
	}

	var fields []string

	// Use Inspector to find SELECT statements and extract fields
	insp := tsqlparser.NewInspector(program)
	for _, stmt := range insp.FindSelectStatements() {
		for _, col := range stmt.Columns {
			if col.AllColumns {
				// SELECT * - we cannot determine fields statically
				continue
			}
			if col.Variable != nil {
				// SELECT @var = expr - the variable name is the output field
				fields = append(fields, strings.TrimPrefix(col.Variable.Name, "@"))
				continue
			}
			if col.Expression == nil {
				continue
			}

			// Use alias if present
			if col.Alias != nil {
				fields = append(fields, col.Alias.Value)
				continue
			}

			// Extract field name from expression based on its type
			fieldName := extractFieldName(col.Expression)
			if fieldName != "" {
				fields = append(fields, fieldName)
			}
		}
	}

	return fields, nil
}

// extractFieldName extracts a field name from an expression.
// For identifiers like "o.order_id", returns "order_id".
// For variables like "@cdc_lsn", returns "cdc_lsn".
func extractFieldName(expr ast.Expression) string {
	if expr == nil {
		return ""
	}

	switch e := expr.(type) {
	case *ast.Identifier:
		return e.Value
	case *ast.QualifiedIdentifier:
		// Take the last part (e.g., o.order_id -> order_id)
		if len(e.Parts) > 0 {
			return e.Parts[len(e.Parts)-1].Value
		}
	case *ast.Variable:
		// @cdc_lsn -> cdc_lsn
		return strings.TrimPrefix(e.Name, "@")
	case *ast.StringLiteral:
		// String literal - no field name
		return ""
	case *ast.IntegerLiteral:
		return ""
	case *ast.FloatLiteral:
		return ""
	case *ast.NullLiteral:
		return ""
	case *ast.FunctionCall:
		// Function call - could return a field name via alias
		return ""
	case *ast.InfixExpression:
		// Binary expression - could be a column reference in some contexts
		// Try left side first
		if left := extractFieldName(e.Left); left != "" {
			return left
		}
		return extractFieldName(e.Right)
	case *ast.PrefixExpression:
		return extractFieldName(e.Right)
	case *ast.CastExpression:
		return extractFieldName(e.Expression)
	case *ast.ConvertExpression:
		return extractFieldName(e.Expression)
	case *ast.CaseExpression:
		// CASE expression - try the first WHEN result
		if len(e.WhenClauses) > 0 {
			return extractFieldName(e.WhenClauses[0].Result)
		}
		if e.ElseClause != nil {
			return extractFieldName(e.ElseClause)
		}
	case *ast.SubqueryExpression:
		return extractFieldName(e.Subquery)
	}

	return ""
}

// sinkIsInsertOrUpdate returns true if the sink's When includes insert or update.
func sinkIsInsertOrUpdate(sink *Sink) bool {
	for _, w := range sink.When {
		if w == "insert" || w == "update" {
			return true
		}
	}
	return false
}

// sinkIsDeleteOnly returns true if the sink's When is delete-only.
func sinkIsDeleteOnly(sink *Sink) bool {
	for _, w := range sink.When {
		if w == "delete" {
			return true
		}
	}
	return false
}

// populateSkillOutputs populates the Outputs map for a skill by analyzing
// the SQL from all insert/update sinks.
func populateSkillOutputs(skill *Skill) error {
	skill.Outputs = make(map[string][]string)

	// Track fields per output table
	outputFields := make(map[string][]string)
	outputSeen := make(map[string]map[string]bool)

	for i := range skill.Sinks {
		sink := &skill.Sinks[i]

		// Skip delete-only sinks
		if sinkIsDeleteOnly(sink) {
			continue
		}

		// Only process insert/update sinks
		if !sinkIsInsertOrUpdate(sink) {
			continue
		}

		// Get SQL content (either inline or from file - file should already be loaded)
		sql := sink.SQL
		if sql == "" {
			continue
		}

		// Extract fields from the SQL
		fields, err := extractOutputFields(sql)
		if err != nil {
			return fmt.Errorf("parse sink SQL for output %q: %w", sink.Output, err)
		}

		// Merge into output fields
		if sink.Output == "" {
			continue
		}

		if outputSeen[sink.Output] == nil {
			outputSeen[sink.Output] = make(map[string]bool)
		}

		for _, field := range fields {
			if !outputSeen[sink.Output][field] {
				outputSeen[sink.Output][field] = true
				outputFields[sink.Output] = append(outputFields[sink.Output], field)
			}
		}
	}

	// Deterministic order: first-seen SQL order (which is preserved in the skill.Sinks iteration order)
	// Fields within each output are already in first-seen order
	skill.Outputs = outputFields
	return nil
}
