package json

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ===================== 基础类型测试 =====================

func TestMarshal_String(t *testing.T) {
	result, err := Marshal("hello")
	require.NoError(t, err)
	assert.Equal(t, `"hello"`, string(result))
}

func TestMarshal_Int(t *testing.T) {
	result, err := Marshal(42)
	require.NoError(t, err)
	assert.Equal(t, `42`, string(result))
}

func TestMarshal_Float64(t *testing.T) {
	result, err := Marshal(3.14)
	require.NoError(t, err)
	assert.Equal(t, `3.14`, string(result))
}

func TestMarshal_Bool(t *testing.T) {
	result, err := Marshal(true)
	require.NoError(t, err)
	assert.Equal(t, `true`, string(result))
}

func TestMarshal_Nil(t *testing.T) {
	result, err := Marshal(nil)
	require.NoError(t, err)
	assert.Equal(t, `null`, string(result))
}

func TestMarshal_Array(t *testing.T) {
	result, err := Marshal([]int{1, 2, 3})
	require.NoError(t, err)
	assert.Equal(t, `[1,2,3]`, string(result))
}

func TestMarshal_Map(t *testing.T) {
	result, err := Marshal(map[string]int{"a": 1, "b": 2})
	require.NoError(t, err)
	assert.True(t, string(result) == `{"a":1,"b":2}` || string(result) == `{"b":2,"a":1}`)
}

// ===================== Struct 测试 =====================

type Person struct {
	Name  string `json:"name"`
	Age   int    `json:"age"`
	Email string `json:"email,omitempty"`
}

func TestMarshal_Struct(t *testing.T) {
	p := Person{Name: "Alice", Age: 30}
	result, err := Marshal(p)
	require.NoError(t, err)
	assert.Equal(t, `{"name":"Alice","age":30}`, string(result))
}

func TestMarshal_Struct_Omitempty(t *testing.T) {
	p := Person{Name: "Alice", Age: 30}
	result, err := Marshal(p)
	require.NoError(t, err)
	assert.NotContains(t, string(result), "email")
}

func TestUnmarshal_Struct(t *testing.T) {
	data := `{"name":"Alice","age":30}`
	var p Person
	err := Unmarshal([]byte(data), &p)
	require.NoError(t, err)
	assert.Equal(t, "Alice", p.Name)
	assert.Equal(t, 30, p.Age)
}

// ===================== 嵌套结构测试 =====================

type Address struct {
	City    string `json:"city"`
	Country string `json:"country"`
}

type User struct {
	Name    string  `json:"name"`
	Address Address `json:"address"`
}

func TestMarshal_NestedStruct(t *testing.T) {
	u := User{Name: "Bob", Address: Address{City: "Beijing", Country: "China"}}
	result, err := Marshal(u)
	require.NoError(t, err)
	assert.Contains(t, string(result), `"name":"Bob"`)
	assert.Contains(t, string(result), `"city":"Beijing"`)
}

func TestUnmarshal_NestedStruct(t *testing.T) {
	data := `{"name":"Bob","address":{"city":"Beijing","country":"China"}}`
	var u User
	err := Unmarshal([]byte(data), &u)
	require.NoError(t, err)
	assert.Equal(t, "Bob", u.Name)
	assert.Equal(t, "Beijing", u.Address.City)
	assert.Equal(t, "China", u.Address.Country)
}

// ===================== 接口类型测试 =====================

func TestMarshal_Interface(t *testing.T) {
	data := map[string]interface{}{
		"name": "test",
		"value": 123,
	}
	result, err := Marshal(data)
	require.NoError(t, err)
	assert.Contains(t, string(result), `"name":"test"`)
}

func TestUnmarshal_Interface(t *testing.T) {
	data := []byte(`{"name":"test","value":123}`)
	var result map[string]interface{}
	err := Unmarshal(data, &result)
	require.NoError(t, err)
	assert.Equal(t, "test", result["name"])
}

// ===================== Encoder/Decoder 测试 =====================

func TestNewEncoder(t *testing.T) {
	buf := &bytes.Buffer{}
	enc := NewEncoder(buf)
	err := enc.Encode(map[string]string{"key": "value"})
	require.NoError(t, err)
	assert.Equal(t, `{"key":"value"}`, strings.TrimSpace(buf.String()))
}

func TestNewDecoder(t *testing.T) {
	reader := strings.NewReader(`{"name":"test","age":25}`)
	dec := NewDecoder(reader)
	var result map[string]interface{}
	err := dec.Decode(&result)
	require.NoError(t, err)
	assert.Equal(t, "test", result["name"])
	assert.Equal(t, float64(25), result["age"])
}

func TestNewDecoder_Multiple(t *testing.T) {
	reader := strings.NewReader(`{"a":1}{"b":2}`)
	dec := NewDecoder(reader)

	var r1, r2 map[string]interface{}
	err := dec.Decode(&r1)
	require.NoError(t, err)
	assert.Equal(t, float64(1), r1["a"])

	err = dec.Decode(&r2)
	require.NoError(t, err)
	assert.Equal(t, float64(2), r2["b"])
}

// ===================== 工具函数测试 =====================

func TestValid(t *testing.T) {
	assert.True(t, Valid([]byte(`{"key":"value"}`)))
	assert.True(t, Valid([]byte(`[1,2,3]`)))
	assert.True(t, Valid([]byte(`"string"`)))
	assert.True(t, Valid([]byte(`123`)))
	assert.True(t, Valid([]byte(`true`)))
	assert.False(t, Valid([]byte(`{invalid}`)))
	assert.False(t, Valid([]byte(``)))
}

func TestCompact(t *testing.T) {
	src := []byte(`{
		"key": "value"
	}`)
	dst := &bytes.Buffer{}
	err := Compact(dst, src)
	require.NoError(t, err)
	assert.Equal(t, `{"key":"value"}`, dst.String())
}

func TestIndent(t *testing.T) {
	src := []byte(`{"key":"value"}`)
	dst := &bytes.Buffer{}
	err := Indent(dst, src, "", "  ")
	require.NoError(t, err)
	expected := `{
  "key": "value"
}`
	assert.Equal(t, expected, dst.String())
}

func TestHTMLEscape(t *testing.T) {
	src := []byte(`{"key":"<script>alert('xss')</script>"}`)
	dst := &bytes.Buffer{}
	HTMLEscape(dst, src)
	assert.NotContains(t, dst.String(), "<script>")
	assert.Contains(t, dst.String(), `\u003cscript\u003e`)
}

func TestMarshalIndent(t *testing.T) {
	data := map[string]string{"name": "Alice", "city": "Beijing"}
	result, err := MarshalIndent(data, "", "  ")
	require.NoError(t, err)
	assert.Contains(t, string(result), `"name": "Alice"`)
	assert.Contains(t, string(result), "\n")
}

// ===================== 错误处理测试 =====================

func TestUnmarshal_InvalidJSON(t *testing.T) {
	data := []byte(`{invalid json}`)
	var result map[string]interface{}
	err := Unmarshal(data, &result)
	assert.Error(t, err)
}

func TestUnmarshal_TypeMismatch(t *testing.T) {
	data := []byte(`123`)
	var result string
	err := Unmarshal(data, &result)
	assert.Error(t, err)
}

func TestUnmarshal_Empty(t *testing.T) {
	data := []byte(``)
	var result map[string]interface{}
	err := Unmarshal(data, &result)
	assert.Error(t, err)
}

func TestMarshal_NilPointer(t *testing.T) {
	var p *Person
	result, err := Marshal(p)
	require.NoError(t, err)
	assert.Equal(t, `null`, string(result))
}

// ===================== 行为一致性测试 =====================

func TestConsistency_Marshal(t *testing.T) {
	data := map[string]interface{}{
		"string": "hello",
		"int":    42,
		"float":  3.14,
		"bool":   true,
		"array":  []int{1, 2, 3},
		"map":    map[string]int{"a": 1},
		"null":   nil,
	}

	// Our marshal
	ourResult, ourErr := Marshal(data)

	// Standard json marshal
	stdResult, stdErr := json.Marshal(data)

	// Should have same error state
	assert.Equal(t, stdErr != nil, ourErr != nil)

	// If both succeed, results should be semantically equal
	if ourErr == nil && stdErr == nil {
		var ourParsed, stdParsed map[string]interface{}
		Unmarshal(ourResult, &ourParsed)
		json.Unmarshal(stdResult, &stdParsed)
		assert.Equal(t, stdParsed, ourParsed)
	}
}

func TestConsistency_Unmarshal(t *testing.T) {
	data := `{"name":"Alice","age":30,"active":true,"score":95.5}`

	var ourResult map[string]interface{}
	ourErr := Unmarshal([]byte(data), &ourResult)

	var stdResult map[string]interface{}
	stdErr := json.Unmarshal([]byte(data), &stdResult)

	// Should have same error state
	assert.Equal(t, stdErr != nil, ourErr != nil)

	// Results should be equal
	if ourErr == nil && stdErr == nil {
		assert.Equal(t, stdResult, ourResult)
	}
}

func TestConsistency_Encoder(t *testing.T) {
	testData := map[string]string{"key": "value"}

	// Our encoder
	ourBuf := &bytes.Buffer{}
	ourEnc := NewEncoder(ourBuf)
	ourEnc.Encode(testData)

	// Standard encoder
	stdBuf := &bytes.Buffer{}
	stdEnc := json.NewEncoder(stdBuf)
	stdEnc.Encode(testData)

	// Results should be equal
	assert.Equal(t, stdBuf.String(), ourBuf.String())
}

func TestConsistency_Decoder(t *testing.T) {
	data := `{"name":"test","value":123}`

	// Our decoder
	ourDec := NewDecoder(strings.NewReader(data))
	var ourResult map[string]interface{}
	ourDec.Decode(&ourResult)

	// Standard decoder
	stdDec := json.NewDecoder(strings.NewReader(data))
	var stdResult map[string]interface{}
	stdDec.Decode(&stdResult)

	// Results should be equal
	assert.Equal(t, stdResult, ourResult)
}

// ===================== RawMessage 测试 =====================

func TestRawMessage(t *testing.T) {
	raw := RawMessage(`{"nested":"value"}`)
	result, err := Marshal(raw)
	require.NoError(t, err)
	assert.Equal(t, `{"nested":"value"}`, string(result))

	var parsed RawMessage
	err = Unmarshal([]byte(`{"nested":"value"}`), &parsed)
	require.NoError(t, err)
	assert.Equal(t, RawMessage(`{"nested":"value"}`), parsed)
}

// ===================== 边界条件测试 ====================

func TestMarshal_EmptyStruct(t *testing.T) {
	result, err := Marshal(struct{}{})
	require.NoError(t, err)
	assert.Equal(t, `{}`, string(result))
}

func TestMarshal_EmptyArray(t *testing.T) {
	result, err := Marshal([]int{})
	require.NoError(t, err)
	assert.Equal(t, `[]`, string(result))
}

func TestMarshal_EmptyMap(t *testing.T) {
	result, err := Marshal(map[string]int{})
	require.NoError(t, err)
	assert.Equal(t, `{}`, string(result))
}

func TestMarshal_Unicode(t *testing.T) {
	result, err := Marshal("你好世界")
	require.NoError(t, err)
	assert.Equal(t, `"你好世界"`, string(result))
}

func TestMarshal_SpecialChars(t *testing.T) {
	result, err := Marshal("line1\nline2\ttab")
	require.NoError(t, err)
	assert.Equal(t, `"line1\nline2\ttab"`, string(result))
}

func TestUnmarshal_Unicode(t *testing.T) {
	data := []byte(`"你好世界"`)
	var result string
	err := Unmarshal(data, &result)
	require.NoError(t, err)
	assert.Equal(t, "你好世界", result)
}
