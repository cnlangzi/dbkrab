package json

import (
	"encoding/json"
	"io"

	"github.com/bytedance/sonic"
)

// ===================== 配置 =====================

// stdAPI uses ConfigStd for deterministic output (SortMapKeys + EscapeHTML)
var stdAPI = sonic.ConfigStd

// ===================== 核心函数 =====================

// Marshal uses ConfigStd for deterministic key ordering
// This is critical for use cases that depend on consistent JSON serialization
// (e.g., hashing for deduplication)
var Marshal = stdAPI.Marshal

// Unmarshal is an alias for sonic.Unmarshal
var Unmarshal = sonic.Unmarshal

// ===================== Encoder/Decoder =====================

// Encoder is an alias for sonic.Encoder
type Encoder = sonic.Encoder

// Decoder is an alias for sonic.Decoder
type Decoder = sonic.Decoder

// NewEncoder returns a new Encoder that writes to w using deterministic stdAPI config
func NewEncoder(w io.Writer) Encoder {
	return stdAPI.NewEncoder(w)
}

// NewDecoder returns a new Decoder that reads from r using deterministic stdAPI config
func NewDecoder(r io.Reader) Decoder {
	return stdAPI.NewDecoder(r)
}

// ===================== 工具函数 =====================

// Valid reports whether data is valid JSON
var Valid = sonic.Valid

// Compact appends to dst the compacted form of src
// Uses standard library since sonic doesn't expose this
var Compact = json.Compact

// Indent appends to dst indented form of src
// Uses standard library since sonic doesn't expose this
var Indent = json.Indent

// HTMLEscape appends HTML-safe version of src to dst
// Uses standard library since sonic doesn't expose this
var HTMLEscape = json.HTMLEscape

// MarshalIndent is like Marshal but applies Indent
var MarshalIndent = stdAPI.MarshalIndent

// ===================== 类型别名 =====================

// RawMessage is an alias for encoding/json.RawMessage for compatibility
// Use this instead of sonic.NoCopyRawMessage to maintain compatibility with stdlib
type RawMessage = json.RawMessage