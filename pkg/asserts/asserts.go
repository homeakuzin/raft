package asserts

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

func Contains[T comparable](t testing.TB, s []T, v T) {
	t.Helper()
	if !slices.Contains(s, v) {
		t.Logf("expected `%v` to contain `%v`", s, v)
		t.Fatal()
	}
}

func NotContains[T comparable](t testing.TB, s []T, v T) {
	t.Helper()
	if slices.Contains(s, v) {
		t.Logf("expected `%v` not to contain `%v`", s, v)
		t.Fatal()
	}
}

func StrContains(t testing.TB, s, substr string) {
	t.Helper()
	if !strings.Contains(s, substr) {
		t.Fatalf("expected to contain `%s`, got `%s`", substr, s)
	}
}

func True(t testing.TB, actual bool) {
	t.Helper()
	Equal(t, true, actual)
}

func False(t testing.TB, actual bool) {
	t.Helper()
	Equal(t, false, actual)
}

func Err(t testing.TB, expected error, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected `%s`, got `nil`", expected.Error())
	}
	if !errors.Is(err, expected) {
		t.Fatalf("expected `%s`, got `%s`", expected.Error(), err.Error())
	}
}

func ErrNil(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Logf("expected `nil`, got `%s`", err.Error())
		t.Fatal()
	}
}

func Slice[E comparable](t testing.TB, expected, actual []E) {
	t.Helper()
	if !slices.Equal(expected, actual) {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
}

func Len[E any](t testing.TB, expected int, actual []E) {
	t.Helper()
	Equal(t, expected, len(actual))
}

func NotEqual[T comparable](t testing.TB, expected, actual T) {
	t.Helper()
	if expected == actual {
		t.Fatalf("expected `%v` not to be `%v`", expected, actual)
	}
}

func Equal[T comparable](t testing.TB, expected, actual T) {
	t.Helper()
	if expected != actual {
		t.Fatalf("expected `%v`, got `%v`", expected, actual)
	}
}

func HasKey[T comparable, E any](t testing.TB, key T, in map[T]E) {
	t.Helper()
	if _, ok := in[key]; !ok {
		t.Fatalf("expected `%v` to have key `%v`", in, key)
	}
}

func NotHasKey[T comparable, E any](t testing.TB, key T, in map[T]E) {
	t.Helper()
	if _, ok := in[key]; ok {
		t.Fatalf("expected `%v` not to have key `%v`", in, key)
	}
}
