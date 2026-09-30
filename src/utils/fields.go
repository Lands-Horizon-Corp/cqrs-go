package utils

import (
	"fmt"
	"reflect"
	"strings"
)

// BunColumnFieldIndex resolves, once, the index of the exported struct
// field on T whose `bun` tag names column. Intended to be resolved once at
// construction time and cached, so hot paths never have to re-scan struct
// tags per call. Returns -1 if no exported field matches.
func BunColumnFieldIndex[T any](column string) int {
	t := reflect.TypeFor[T]()
	if t.Kind() != reflect.Struct {
		return -1
	}
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}
		tag := field.Tag.Get("bun")
		if tag == "" || tag == "-" {
			continue
		}
		name, _, _ := strings.Cut(tag, ",")
		if name == column {
			return i
		}
	}
	return -1
}

// FieldValueAt reads the field at idx (as resolved by BunColumnFieldIndex)
// off data and returns it formatted as a string. It is safe to call with a
// nil data pointer or an out-of-range/negative idx.
func FieldValueAt[TData any](data *TData, idx int) string {
	if data == nil || idx < 0 {
		return ""
	}
	v := reflect.ValueOf(data).Elem()
	if idx >= v.NumField() {
		return ""
	}
	f := v.Field(idx)
	if !f.CanInterface() {
		return ""
	}
	return formatFieldValue(f)
}

// FieldIsNilAt reports whether the field at idx (as resolved by
// BunColumnFieldIndex) is absent — a nil pointer/interface/slice/map/chan/
// func, the same notion of "absent" formatFieldValue already uses to
// collapse such a field to "" instead of the literal text "<nil>". A
// non-nilable field (int, string, bool, struct, ...) is never absent.
// Safe to call with a nil data pointer or an out-of-range/negative idx —
// both count as absent, matching FieldValueAt's own safe-default behavior.
func FieldIsNilAt[TData any](data *TData, idx int) bool {
	if data == nil || idx < 0 {
		return true
	}
	v := reflect.ValueOf(data).Elem()
	if idx >= v.NumField() {
		return true
	}
	f := v.Field(idx)
	switch f.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
		return !f.CanInterface() || f.IsNil()
	default:
		return false
	}
}

// formatFieldValue turns a struct field's reflect.Value into a string,
// treating a nil pointer/interface/slice/map/chan/func as an absent value
// ("") rather than the literal text "<nil>", and dereferencing a non-nil
// pointer/interface instead of stringifying the pointer itself — fmt's
// default %v formatting only auto-dereferences pointers to struct/slice/
// map, so a *string or *bool would otherwise print as a hex address.
func formatFieldValue(f reflect.Value) string {
	switch f.Kind() {
	case reflect.String:
		return f.String()
	case reflect.Pointer, reflect.Interface:
		if f.IsNil() {
			return ""
		}
		return formatFieldValue(f.Elem())
	case reflect.Slice, reflect.Map, reflect.Chan, reflect.Func:
		if f.IsNil() {
			return ""
		}
	}
	return fmt.Sprintf("%v", f.Interface())
}
