package mqtt

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	jsoniter "github.com/json-iterator/go"
)

type framer struct {
	// selected leaf paths (slash notation, e.g. "stats/totalTimeMs"). When empty the
	// framer falls back to the classic behavior of extracting every top-level key.
	selected []string

	path     []string
	iterator *jsoniter.Iterator
	fields   []*data.Field
	fieldMap map[string]int
}

func (df *framer) next(logger log.Logger) error {
	switch df.iterator.WhatIsNext() {
	case jsoniter.StringValue:
		v := df.iterator.ReadString()
		df.addValue(data.FieldTypeNullableString, &v)
	case jsoniter.NumberValue:
		v := df.iterator.ReadFloat64()
		df.addValue(data.FieldTypeNullableFloat64, &v)
	case jsoniter.BoolValue:
		v := df.iterator.ReadBool()
		df.addValue(data.FieldTypeNullableBool, &v)
	case jsoniter.NilValue:
		df.addNil(logger)
		df.iterator.ReadNil()
	case jsoniter.ArrayValue:
		df.addValue(data.FieldTypeJSON, json.RawMessage(df.iterator.SkipAndReturnBytes()))
	case jsoniter.ObjectValue:
		size := len(df.path)
		if size > 0 {
			df.addValue(data.FieldTypeJSON, json.RawMessage(df.iterator.SkipAndReturnBytes()))
			break
		}
		for fname := df.iterator.ReadObject(); fname != ""; fname = df.iterator.ReadObject() {
			if size == 0 {
				df.path = append(df.path, fname)
				if err := df.next(logger); err != nil {
					return err
				}
			}
		}
	case jsoniter.InvalidValue:
		return fmt.Errorf("invalid value")
	}
	df.path = []string{}
	return nil
}

func (df *framer) key() string {
	if len(df.path) == 0 {
		return "Value"
	}
	return strings.Join(df.path, "")
}

func (df *framer) addNil(logger log.Logger) {
	if idx, ok := df.fieldMap[df.key()]; ok {
		df.fields[idx].Set(0, nil)
		return
	}
	logger.Debug("nil value for unknown field", "key", df.key())
}

func (df *framer) addValue(fieldType data.FieldType, v interface{}) {
	if idx, ok := df.fieldMap[df.key()]; ok {
		if df.fields[idx].Type() != fieldType {
			log.DefaultLogger.Debug("field type mismatch", "key", df.key(), "existing", df.fields[idx], "new", fieldType)
			return
		}
		df.fields[idx].Append(v)
		return
	}
	field := data.NewFieldFromFieldType(fieldType, df.fields[0].Len())
	field.Name = df.key()
	field.Append(v)
	df.fields = append(df.fields, field)
	df.fieldMap[df.key()] = len(df.fields) - 1
}

func newFramer(selected ...string) *framer {
	df := &framer{
		fieldMap: make(map[string]int),
		selected: selected,
	}
	timeField := data.NewFieldFromFieldType(data.FieldTypeTime, 0)
	timeField.Name = "Time"
	df.fields = append(df.fields, timeField)
	df.fieldMap["Time"] = 0

	// In selection mode pre-create one column per selected path so the frame schema
	// is deterministic (present even when a message omits the field). This keeps the
	// seed frame and the streamed frames schema-compatible so Grafana can append.
	for _, p := range selected {
		field := data.NewFieldFromFieldType(data.FieldTypeNullableFloat64, 0)
		field.Name = p
		df.fields = append(df.fields, field)
		df.fieldMap[p] = len(df.fields) - 1
	}
	return df
}

// toFrame builds a data.Frame from the given messages. NOTE: it reuses the framer's own
// *data.Field slice across calls (cleared at the top and returned inside the frame), so the
// returned frame ALIASES framer state — it is only valid until the next Seed/Stream call. This is
// safe today because RunStream calls SendFrame synchronously (the SDK serializes the frame before
// returning) and SeedFrame/StreamDelta serialize on stream.mu. If any future code RETAINS a
// returned frame past that window (e.g. buffering frames), copy it first (data.Frame has no deep
// copy — rebuild fresh fields) or build fresh fields here.
func (df *framer) toFrame(messages []Message, logger log.Logger) (*data.Frame, error) {
	// clear the data in the fields
	for _, field := range df.fields {
		for i := field.Len() - 1; i >= 0; i-- {
			field.Delete(i)
		}
	}

	for _, message := range messages {
		if len(df.selected) > 0 {
			df.appendSelected(message.Value, logger)
			df.fields[0].Append(message.Timestamp)
			df.extendFields(df.fields[0].Len() - 1)
			continue
		}

		df.iterator = jsoniter.ParseBytes(jsoniter.ConfigDefault, message.Value)
		err := df.next(logger)
		if err != nil {
			// If JSON parsing fails, treat the raw bytes as a string value
			logger.Debug("JSON parsing failed, treating as raw string", "error", err, "value", string(message.Value))
			rawValue := string(message.Value)
			df.addValue(data.FieldTypeNullableString, &rawValue)
		}
		df.fields[0].Append(message.Timestamp)
		df.extendFields(df.fields[0].Len() - 1)
	}

	return data.NewFrame("mqtt", df.fields...), nil
}

// appendSelected extracts each configured leaf path from a single JSON message and
// appends its value to the matching pre-created column. Missing paths get a nil.
func (df *framer) appendSelected(payload []byte, logger log.Logger) {
	var root interface{}
	if err := json.Unmarshal(payload, &root); err != nil {
		logger.Debug("selection: JSON parse failed", "error", err, "value", string(payload))
		root = nil
	}
	for _, p := range df.selected {
		idx := df.fieldMap[p]
		leaf, ok := lookupPath(root, p)
		if !ok || leaf == nil {
			df.fields[idx].Append(nil)
			continue
		}
		df.appendTyped(idx, leaf)
	}
}

// appendTyped appends a decoded JSON leaf to the column at idx. Columns are created
// as nullable float64; a leaf whose type does not match the column is appended as nil
// rather than dropped or panicking (mixed-type metric streams are unusual).
func (df *framer) appendTyped(idx int, leaf interface{}) {
	f := df.fields[idx]
	if v, ok := leaf.(float64); ok && f.Type() == data.FieldTypeNullableFloat64 {
		val := v
		f.Append(&val)
		return
	}
	f.Append(nil)
}

func (df *framer) extendFields(idx int) {
	for _, f := range df.fields {
		if idx+1 > f.Len() {
			f.Extend(idx + 1 - f.Len())
		}
	}
}

// lookupPath walks a slash-separated path into a decoded JSON value and returns the
// leaf. Only object traversal is supported (arrays are treated as leaves/JSON).
func lookupPath(root interface{}, path string) (interface{}, bool) {
	cur := root
	for _, seg := range strings.Split(path, "/") {
		obj, ok := cur.(map[string]interface{})
		if !ok {
			return nil, false
		}
		cur, ok = obj[seg]
		if !ok {
			return nil, false
		}
	}
	return cur, true
}

// FlattenLeafPaths returns every scalar leaf path (slash notation) found in a JSON
// sample message, sorted. Used by discovery to offer a field pick-list. Objects are
// descended into; arrays and scalars are treated as leaves.
func FlattenLeafPaths(payload []byte) ([]string, error) {
	var root interface{}
	if err := json.Unmarshal(payload, &root); err != nil {
		return nil, err
	}
	var out []string
	var walk func(prefix string, v interface{})
	walk = func(prefix string, v interface{}) {
		if obj, ok := v.(map[string]interface{}); ok && len(obj) > 0 {
			for k, child := range obj {
				next := k
				if prefix != "" {
					next = prefix + "/" + k
				}
				walk(next, child)
			}
			return
		}
		if prefix != "" {
			out = append(out, prefix)
		}
	}
	walk("", root)
	sort.Strings(out)
	return out, nil
}
