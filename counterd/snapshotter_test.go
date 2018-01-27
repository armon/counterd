package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFilterKeys(t *testing.T) {
	p1, _ := ParseKey("day:2017-01-18:foo:bar")
	p2, _ := ParseKey("day:2017-01-10:foo:bar")
	p3, _ := ParseKey("day:2017-01-01:foo:bar")

	inp := []*ParsedKey{p1, p2, p3}
	updateThres := time.Date(2017, 1, 17, 0, 0, 0, 0, time.UTC)
	deleteThres := time.Date(2017, 1, 9, 0, 0, 0, 0, time.UTC)
	update, ignore, delete := FilterKeys(inp, updateThres, deleteThres)

	assert.Contains(t, update, p1)
	assert.Contains(t, ignore, p2)
	assert.Contains(t, delete, p3)
}

func TestParseKeyList(t *testing.T) {
	input := []string{
		"day:2017-01-18:foo:bar",
		"week:2017-12-18:foo:bar:zip:zap",
		"month",
	}
	out, invalid := ParseKeyList(input)
	assert.Equal(t, 2, len(out))
	assert.Equal(t, 1, len(invalid))
}

func TestParseKey(t *testing.T) {
	type tcase struct {
		Input    string
		Expected *ParsedKey
		Err      string
	}
	tcases := []tcase{
		{
			Input: "day:2017-01-18:foo:bar",
			Expected: &ParsedKey{
				Interval: "day",
				Date:     time.Date(2017, 1, 18, 0, 0, 0, 0, time.UTC),
				Attributes: map[string]string{
					"foo": "bar",
				},
			},
		},
		{
			Input: "week:2017-12-18:foo:bar:zip:zap",
			Expected: &ParsedKey{
				Interval: "week",
				Date:     time.Date(2017, 12, 18, 0, 0, 0, 0, time.UTC),
				Attributes: map[string]string{
					"foo": "bar",
					"zip": "zap",
				},
			},
		},
		{
			Input: "month:2017-12:foo:bar:zip:zap",
			Expected: &ParsedKey{
				Interval: "month",
				Date:     time.Date(2017, 12, 1, 0, 0, 0, 0, time.UTC),
				Attributes: map[string]string{
					"foo": "bar",
					"zip": "zap",
				},
			},
		},
		{
			Input: "month:2017:foo:bar:zip:zap",
			Err:   "invalid date \"2017\"",
		},
		{
			Input: "foo:2017:foo:bar:zip:zap",
			Err:   "invalid interval \"foo\"",
		},
		{
			Input: "month:2017-12:foo:bar:zip",
			Err:   "key/value attributes not even",
		},
		{
			Input: "month:zip",
			Err:   "invalid format",
		},
	}

	for _, tc := range tcases {
		out, err := ParseKey(tc.Input)
		if tc.Err == "" {
			assert.Nil(t, err)
			tc.Expected.Raw = tc.Input
			assert.Equal(t, tc.Expected, out)
		} else {
			assert.Equal(t, tc.Err, err.Error())
		}
	}
}
