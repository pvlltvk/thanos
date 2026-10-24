// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package promclient

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/efficientgo/core/testutil"
	"github.com/gogo/status"
	"github.com/prometheus/prometheus/model/labels"
)

func TestSeriesInGRPCStream(t *testing.T) {
	base, err := url.Parse("http://prometheus.test/prefix")
	testutil.Ok(t, err)
	matcher := labels.MustNewMatcher(labels.MatchEqual, "job", "api")
	for _, tc := range []struct {
		name, body, diagnostic string
		want                   []map[string]string
		wantErr                bool
	}{
		{name: "status first", body: `{"status":"success","data":[{"job":"api","instance":"a"},{"job":"api","instance":"b"}]}`, want: []map[string]string{{"job": "api", "instance": "a"}, {"job": "api", "instance": "b"}}},
		{name: "data first and unknown fields", body: `{"unknown":[true,null,{"value":42}],"data":[{"job":"api"}],"status":"success"}`, want: []map[string]string{{"job": "api"}}},
		{name: "case insensitive envelope", body: `{"DATA":[{"JOB":"api"}],"StAtUs":"success"}`, want: []map[string]string{{"JOB": "api"}}},
		{name: "duplicate data replaces", body: `{"status":"success","data":[{"job":"old"},{"job":"discard"}],"data":[{"job":"new"}]}`, want: []map[string]string{{"job": "new"}}},
		{name: "null data replaces", body: `{"status":"success","data":[{"job":"old"}],"data":null}`},
		{name: "null then array", body: `{"status":"success","data":null,"data":[{"job":"new"}]}`, want: []map[string]string{{"job": "new"}}},
		{name: "empty data", body: `{"status":"success","data":[]}`},
		{name: "missing data", body: `{"status":"success"}`},
		{name: "null row and label", body: `{"status":"success","data":[null,{"job":null}]}`, want: []map[string]string{nil, {"job": ""}}},
		{name: "null status retains string", body: `{"status":"success","status":null,"data":[{"job":"api"}]}`, want: []map[string]string{{"job": "api"}}},
		{name: "duplicate scalar last string", body: `{"status":"error","error":"old","status":"success","data":[{"job":"api"}]}`, want: []map[string]string{{"job": "api"}}},
		{name: "status failure after data", body: `{"data":[{"job":"api"}],"status":"error","error":"upstream rejected"}`, wantErr: true, diagnostic: "upstream rejected"},
		{name: "status outranks typed data", body: `{"data":[{"job":true}],"status":"error","error":"upstream rejected"}`, wantErr: true, diagnostic: "upstream rejected"},
		{name: "malformed after status error", body: `{"status":"error","error":"upstream rejected","data":[`, wantErr: true},
		{name: "trailing document", body: `{"status":"success","data":[]} {}`, wantErr: true},
		{name: "wrong data type", body: `{"status":"success","data":{}}`, wantErr: true, diagnostic: "cannot unmarshal"},
		{name: "wrong row type", body: `{"status":"success","data":[1]}`, wantErr: true, diagnostic: "cannot unmarshal"},
		{name: "wrong label type", body: `{"status":"success","data":[{"job":true}]}`, wantErr: true, diagnostic: "cannot unmarshal"},
		{name: "earlier data schema error survives replacement", body: `{"status":"success","data":[{"job":true}],"data":[{"job":"api"}]}`, wantErr: true, diagnostic: "cannot unmarshal"},
		{name: "earlier scalar schema error survives replacement", body: `{"status":42,"status":"success","data":[]}`, wantErr: true, diagnostic: "cannot unmarshal"},
		{name: "error field schema even on success", body: `{"status":"success","error":42,"data":[]}`, wantErr: true, diagnostic: "cannot unmarshal"},
		{name: "missing status", body: `{"data":[]}`, wantErr: true},
		{name: "null envelope", body: `null`, wantErr: true},
		{name: "array envelope", body: `[]`, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			legacyBody := &seriesStreamTestBody{data: []byte(tc.body), chunk: 23}
			legacy := seriesStreamTestClient(legacyBody, http.StatusOK, nil)
			legacyRows, legacyErr := legacy.SeriesInGRPC(context.Background(), base, []*labels.Matcher{matcher}, 1234, 5678, 7)
			if (legacyErr != nil) != tc.wantErr {
				t.Fatalf("legacy error=%v, wantErr=%t", legacyErr, tc.wantErr)
			}
			if !tc.wantErr {
				testutil.Equals(t, len(tc.want), len(legacyRows))
				for i := range tc.want {
					testutil.Equals(t, tc.want[i], legacyRows[i])
				}
			}
			body := &seriesStreamTestBody{data: []byte(tc.body), chunk: 23}
			client := seriesStreamTestClient(body, http.StatusOK, func(request *http.Request) {
				testutil.Equals(t, "/prefix/api/v1/series", request.URL.Path)
				testutil.Equals(t, `{job="api"}`, request.URL.Query().Get("match[]"))
				testutil.Equals(t, "1.234", request.URL.Query().Get("start"))
				testutil.Equals(t, "5.678", request.URL.Query().Get("end"))
				testutil.Equals(t, "7", request.URL.Query().Get("limit"))
				testutil.Equals(t, "stream-test", request.UserAgent())
			})
			var rows []map[string]string
			err := client.SeriesInGRPCStream(context.Background(), base, []*labels.Matcher{matcher}, 1234, 5678, 7,
				func() error { rows = nil; return nil }, func(row map[string]string) error { rows = append(rows, row); return nil })
			if (err != nil) != tc.wantErr {
				t.Fatalf("stream error=%v, wantErr=%t", err, tc.wantErr)
			}
			testutil.Equals(t, status.Code(legacyErr), status.Code(err))
			if tc.wantErr {
				if tc.diagnostic != "" && !strings.Contains(err.Error(), tc.diagnostic) {
					t.Fatalf("error %v does not contain %q", err, tc.diagnostic)
				}
			} else {
				testutil.Equals(t, len(tc.want), len(rows))
				for i := range tc.want {
					testutil.Equals(t, tc.want[i], rows[i])
				}
			}
			testutil.Equals(t, len(body.data), body.position)
			testutil.Assert(t, body.closed, "response body was not closed")
		})
	}

	t.Run("calls back before later entries are read", func(t *testing.T) {
		payload := `{"data":[{"job":"a"},{"job":"b"}],"status":"success","unknown":"` + strings.Repeat("x", 1024) + `"}`
		boundary := strings.Index(payload, `},{`) + 1
		body := &seriesStreamTestBody{data: []byte(payload), boundary: boundary}
		client := seriesStreamTestClient(body, http.StatusOK, nil)
		calls := 0
		testutil.Ok(t, client.SeriesInGRPCStream(context.Background(), base, nil, 0, 1, 0, func() error { return nil }, func(row map[string]string) error {
			calls++
			if calls == 1 {
				testutil.Assert(t, body.position < len(body.data), "first callback occurred only after full-body consumption")
				testutil.Equals(t, "a", row["job"])
			}
			return nil
		}))
		testutil.Equals(t, 2, calls)
		testutil.Equals(t, len(body.data), body.position)
		testutil.Assert(t, body.closed, "response body was not closed")
	})

	t.Run("callback failure consumes input and preserves cause", func(t *testing.T) {
		callbackErr := errors.New("callback stopped")
		body := &seriesStreamTestBody{data: []byte(`{"data":[{"job":"a"},{"job":"b"}],"status":"success"}`), chunk: 13}
		calls := 0
		err := seriesStreamTestClient(body, http.StatusOK, nil).SeriesInGRPCStream(context.Background(), base, nil, 0, 1, 0,
			func() error { return nil }, func(map[string]string) error { calls++; return callbackErr })
		testutil.Assert(t, err == callbackErr, "callback error was not returned unchanged: %v", err)
		testutil.Equals(t, 1, calls)
		testutil.Equals(t, len(body.data), body.position)
		testutil.Assert(t, body.closed, "response body was not closed")
	})

	t.Run("later validation outranks callback failure", func(t *testing.T) {
		for _, payload := range []string{
			`{"data":[{"job":"a"},{"job":"b"}],"status":"error","error":"upstream rejected"}`,
			`{"data":[{"job":"a"},{"job":"b"}],"status":"success"} trailing`,
		} {
			callbackErr := errors.New("callback stopped")
			body := &seriesStreamTestBody{data: []byte(payload), chunk: 13}
			err := seriesStreamTestClient(body, http.StatusOK, nil).SeriesInGRPCStream(context.Background(), base, nil, 0, 1, 0,
				func() error { return nil }, func(map[string]string) error { return callbackErr })
			testutil.NotOk(t, err)
			testutil.Assert(t, !errors.Is(err, callbackErr), "callback error took precedence over upstream validation")
			testutil.Equals(t, len(body.data), body.position)
			testutil.Assert(t, body.closed, "response body was not closed")
		}
	})

	t.Run("duplicate data clears earlier callback failure", func(t *testing.T) {
		body := &seriesStreamTestBody{data: []byte(`{"data":[{"job":"reject"}],"data":[{"job":"kept"}],"status":"success"}`), chunk: 17}
		var rows []map[string]string
		resets := 0
		testutil.Ok(t, seriesStreamTestClient(body, http.StatusOK, nil).SeriesInGRPCStream(context.Background(), base, nil, 0, 1, 0,
			func() error { resets++; rows = nil; return nil }, func(row map[string]string) error {
				if row["job"] == "reject" {
					return errors.New("first occurrence stopped")
				}
				rows = append(rows, row)
				return nil
			}))
		testutil.Equals(t, 2, resets)
		testutil.Equals(t, []map[string]string{{"job": "kept"}}, rows)
	})

	t.Run("reset failure consumes input", func(t *testing.T) {
		resetErr := errors.New("reset failed")
		body := &seriesStreamTestBody{data: []byte(`{"data":[{"job":"a"}],"status":"success"}`), chunk: 11}
		calls := 0
		err := seriesStreamTestClient(body, http.StatusOK, nil).SeriesInGRPCStream(context.Background(), base, nil, 0, 1, 0,
			func() error { return resetErr }, func(map[string]string) error { calls++; return nil })
		testutil.Assert(t, err == resetErr, "reset error was not returned unchanged: %v", err)
		testutil.Equals(t, 0, calls)
		testutil.Equals(t, len(body.data), body.position)
		testutil.Assert(t, body.closed, "response body was not closed")
	})

	for _, tc := range []struct {
		name       string
		code       int
		body       string
		readErr    error
		closeErr   error
		diagnostic string
	}{
		{name: "read error after complete json", code: http.StatusOK, body: `{"status":"success","data":[{"job":"api"}]}`, readErr: errors.New("read stopped"), diagnostic: "read stopped"},
		{name: "close error", code: http.StatusOK, body: `{"status":"success","data":[]}`, closeErr: errors.New("close stopped"), diagnostic: "close stopped"},
		{name: "close error before status error", code: http.StatusOK, body: `{"status":"error","error":"upstream rejected","data":[]}`, closeErr: errors.New("close stopped"), diagnostic: "close stopped"},
		{name: "http error before parsing", code: http.StatusNotFound, body: `invalid json`, diagnostic: "expected 2xx"},
		{name: "no content", code: http.StatusNoContent, body: ``},
	} {
		t.Run(tc.name, func(t *testing.T) {
			legacyBody := &seriesStreamTestBody{data: []byte(tc.body), readErr: tc.readErr, closeErr: tc.closeErr}
			_, legacyErr := seriesStreamTestClient(legacyBody, tc.code, nil).SeriesInGRPC(context.Background(), base, nil, 0, 1, 0)
			body := &seriesStreamTestBody{data: []byte(tc.body), readErr: tc.readErr, closeErr: tc.closeErr}
			err := seriesStreamTestClient(body, tc.code, nil).SeriesInGRPCStream(context.Background(), base, nil, 0, 1, 0,
				func() error { return nil }, func(map[string]string) error { return nil })
			testutil.Equals(t, status.Code(legacyErr), status.Code(err))
			if tc.diagnostic != "" {
				testutil.NotOk(t, err)
				testutil.Assert(t, strings.Contains(err.Error(), tc.diagnostic), "missing cause %q: %v", tc.diagnostic, err)
			} else {
				testutil.Ok(t, err)
			}
			testutil.Assert(t, body.closed, "response body was not closed")
		})
	}

	t.Run("request failure preserves grpc error", func(t *testing.T) {
		requestErr := errors.New("request stopped")
		client := NewClient(metadataMemoryRoundTripper(func(*http.Request) (*http.Response, error) { return nil, requestErr }), nil, "")
		_, legacyErr := client.SeriesInGRPC(context.Background(), base, nil, 0, 1, 0)
		err := client.SeriesInGRPCStream(context.Background(), base, nil, 0, 1, 0, func() error { return nil }, func(map[string]string) error { return nil })
		testutil.NotOk(t, err)
		testutil.Equals(t, status.Code(legacyErr), status.Code(err))
		testutil.Assert(t, strings.Contains(err.Error(), requestErr.Error()), "request failure cause lost: %v", err)
	})

	t.Run("cancellation closes response", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		body := &seriesStreamTestBody{data: []byte(`{"data":[{"job":"a"},{"job":"b"}],"status":"success"}`), chunk: 5, ctx: ctx}
		err := seriesStreamTestClient(body, http.StatusOK, nil).SeriesInGRPCStream(ctx, base, nil, 0, 1, 0,
			func() error { return nil }, func(map[string]string) error { cancel(); return nil })
		testutil.NotOk(t, err)
		testutil.Assert(t, strings.Contains(err.Error(), context.Canceled.Error()), "cancellation cause lost: %v", err)
		testutil.Assert(t, body.closed, "response body was not closed")
	})
	t.Run("duplicate data replaces labels instead of merging", func(t *testing.T) {
		const payload = `{"data":[{"job":"old","extra":"keep"}],"data":[{"job":"new"}],"status":"success"}`
		legacyBody := &seriesStreamTestBody{data: []byte(payload), chunk: 17}
		legacyRows, err := seriesStreamTestClient(legacyBody, http.StatusOK, nil).SeriesInGRPC(context.Background(), base, nil, 0, 1, 0)
		testutil.Ok(t, err)
		testutil.Equals(t, []map[string]string{{"job": "new", "extra": "keep"}}, legacyRows)

		body := &seriesStreamTestBody{data: []byte(payload), chunk: 17}
		var rows []map[string]string
		testutil.Ok(t, seriesStreamTestClient(body, http.StatusOK, nil).SeriesInGRPCStream(context.Background(), base, nil, 0, 1, 0,
			func() error { rows = nil; return nil }, func(row map[string]string) error { rows = append(rows, row); return nil }))
		testutil.Equals(t, []map[string]string{{"job": "new"}}, rows)
		testutil.Equals(t, len(body.data), body.position)
		testutil.Assert(t, body.closed, "response body was not closed")
	})

}

func seriesStreamTestClient(body io.ReadCloser, code int, inspect func(*http.Request)) *Client {
	return NewClient(metadataMemoryRoundTripper(func(request *http.Request) (*http.Response, error) {
		if inspect != nil {
			inspect(request)
		}
		return &http.Response{StatusCode: code, Header: make(http.Header), Body: body}, nil
	}), nil, "stream-test")
}

type seriesStreamTestBody struct {
	data              []byte
	position, chunk   int
	boundary          int
	closed            bool
	readErr, closeErr error
	ctx               context.Context
}

func (b *seriesStreamTestBody) Read(p []byte) (int, error) {
	if b.ctx != nil && b.ctx.Err() != nil {
		return 0, b.ctx.Err()
	}
	if b.position == len(b.data) {
		if b.readErr != nil {
			return 0, b.readErr
		}
		return 0, io.EOF
	}
	end := len(b.data)
	if b.chunk > 0 && end > b.position+b.chunk {
		end = b.position + b.chunk
	}
	if b.boundary > b.position && end > b.boundary {
		end = b.boundary
	}
	n := copy(p, b.data[b.position:end])
	b.position += n
	return n, nil
}

func (b *seriesStreamTestBody) Close() error {
	b.closed = true
	return b.closeErr
}
