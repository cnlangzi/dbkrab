package json

import (
	"encoding/json"
	"io"

	"github.com/bytedance/sonic"
)

// ===================== 核心函数 =====================

// Marshal is an alias for sonic.Marshal
var Marshal = sonic.Marshal

// Unmarshal is an alias for sonic.Unmarshal
var Unmarshal = sonic.Unmarshal

// ===================== Encoder/Decoder =====================

// Encoder is an alias for sonic.Encoder
type Encoder = sonic.Encoder

// Decoder is an alias for sonic.Decoder
type Decoder = sonic.Decoder

// NewEncoder returns a new Encoder that writes to w
func NewEncoder(w io.Writer) Encoder {
	return sonic.ConfigDefault.NewEncoder(w)
}

// NewDecoder returns a new Decoder that reads from r
func NewDecoder(r io.Reader) Decoder {
	return sonic.ConfigDefault.NewDecoder(r)
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
var MarshalIndent = sonic.MarshalIndent

// ===================== 类型别名 =====================

// RawMessage is an alias for sonic.NoCopyRawMessage
type RawMessage = sonic.NoCopyRawMessage
