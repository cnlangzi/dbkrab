package json

import (
	"testing"
)

func TestSonicKeyOrdering(t *testing.T) {
	// Test case: map with multiple keys
	data := map[string]interface{}{
		"z": "z",
		"a": "a",
		"m": "m",
		"b": "b",
	}

	// Test our wrapper's Marshal (which now uses ConfigStd)
	result1, err := Marshal(data)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}
	t.Logf("internal/json.Marshal: %s", string(result1))

	// Verify deterministic - run multiple times
	for i := 0; i < 5; i++ {
		r, _ := Marshal(data)
		if string(r) != string(result1) {
			t.Fatalf("Non-deterministic! iteration %d: %s", i, string(r))
		}
	}
	t.Log("Deterministic: YES ✓")
	
	// Verify sorted order (alphabetical)
	expected := `{"a":"a","b":"b","m":"m","z":"z"}`
	if string(result1) != expected {
		t.Logf("Expected: %s", expected)
		t.Fatalf("Got: %s", string(result1))
	}
}
