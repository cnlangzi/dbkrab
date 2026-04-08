package sql

import (
	"fmt"
)

// Router routes SQL results to appropriate sinks
type Router struct{}

// NewRouter creates a new result router
func NewRouter() *Router {
	return &Router{}
}

// RouteResult routes a DataSet to the appropriate sink configuration
// For multi-table CDC, it filters sinks by table name
func (r *Router) RouteResult(ds *DataSet, sinks []SinkConfig, tableName string) map[string]*DataSet {
	result := make(map[string]*DataSet)

	// Filter sinks by table name if specified
	filtered := FilterSinks(sinks, tableName)

	for _, sink := range filtered {
		if sink.Output == "" {
			continue
		}
		// For simple SELECT * queries, use the entire DataSet
		// For more complex queries with specific SQL, the SQL has already been executed
		result[sink.Output] = ds
	}

	return result
}

// RouteBySQL routes results based on sink SQL definitions
// This executes the SQL for each sink and returns the results
func (r *Router) RouteBySQL(executor *Executor, ds *DataSet, sinks []SinkConfig, tableName string) (map[string]*DataSet, error) {
	result := make(map[string]*DataSet)

	// Filter sinks by table name if specified
	filtered := FilterSinks(sinks, tableName)

	for _, sink := range filtered {
		if sink.Output == "" {
			continue
		}

		// If sink has SQL, execute it
		if sink.SQL != "" {
			execDS, err := executor.ExecuteInline(sink.SQL)
			if err != nil {
				return nil, NewRoutingError(sink.Name, err)
			}
			result[sink.Output] = execDS
		} else {
			// No SQL specified, use the input DataSet
			result[sink.Output] = ds
		}
	}

	return result, nil
}

// GetSinkByName finds a sink configuration by name
func (r *Router) GetSinkByName(sinks []SinkConfig, name string) *SinkConfig {
	for i := range sinks {
		if sinks[i].Name == name {
			return &sinks[i]
		}
	}
	return nil
}

// RouteMultiTable handles routing for multi-table CDC transactions
// It groups results by sink and returns a map of table name to DataSet
func (r *Router) RouteMultiTable(results map[string]*DataSet, sinks []SinkConfig) map[string]*DataSet {
	routed := make(map[string]*DataSet)

	for _, sink := range sinks {
		if sink.Output == "" {
			continue
		}

		// Check if we have results from stages or need to route from main results
		// For multi-table, each sink has its own SQL that should be executed
		for tableName, ds := range results {
			// Check if this result matches the sink's table filter
			if sink.On == "" || sink.On == tableName {
				routed[sink.Output] = ds
			}
		}
	}

	return routed
}

// SplitDataSet splits a DataSet into multiple DataSets based on sink configurations
// Used for fan-out scenarios where one CDC event writes to multiple tables
func (r *Router) SplitDataSet(ds *DataSet, sinks []SinkConfig) map[string]*DataSet {
	result := make(map[string]*DataSet)

	for _, sink := range sinks {
		if sink.Output == "" {
			continue
		}
		// For fan-out, each sink gets a copy of the DataSet
		result[sink.Output] = &DataSet{
			Columns: ds.Columns,
			Rows:    ds.Rows,
		}
	}

	return result
}

// ValidateSinks validates that sink configurations are valid
func (r *Router) ValidateSinks(sinks []SinkConfig) error {
	for _, sink := range sinks {
		if sink.Output == "" {
			return fmt.Errorf("%w: sink %s has no output table", ErrInvalidConfig, sink.Name)
		}
		if sink.PrimaryKey == "" {
			return fmt.Errorf("%w: sink %s has no primary_key", ErrInvalidConfig, sink.Name)
		}
	}
	return nil
}
