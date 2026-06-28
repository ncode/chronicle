package query

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestParseFilter(t *testing.T) {
	q, err := Parse(`role=web os.name=Debian`)
	if err != nil {
		t.Fatal(err)
	}
	if q.Shape != ShapeFilter || len(q.Terms) != 2 || q.At != nil {
		t.Fatalf("parsed = %+v", q)
	}
	if q.Terms[0].Path != "role" || q.Terms[0].Value != "web" {
		t.Fatalf("term0 = %+v", q.Terms[0])
	}
}

func TestParseTypedLiterals(t *testing.T) {
	q, err := Parse(`a=1 b="1" c=true d=null e='x y'`)
	if err != nil {
		t.Fatal(err)
	}
	want := []any{json.Number("1"), "1", true, nil, "x y"}
	for i, w := range want {
		if q.Terms[i].Value != w {
			t.Errorf("term %d value = %#v, want %#v", i, q.Terms[i].Value, w)
		}
	}
}

func TestParseGroupBy(t *testing.T) {
	q, err := Parse(`role where os.name='Debian' group by role`)
	if err != nil {
		t.Fatal(err)
	}
	if q.Shape != ShapeGroupBy || q.GroupField != "role" {
		t.Fatalf("parsed = %+v", q)
	}
	if len(q.Terms) != 1 || q.Terms[0].Path != "os.name" || q.Terms[0].Value != "Debian" {
		t.Fatalf("terms = %+v", q.Terms)
	}
}

func TestParseAt(t *testing.T) {
	q, err := Parse(`os.name=Debian at 2026-03-14T06:00:00Z`)
	if err != nil {
		t.Fatal(err)
	}
	if q.At == nil || q.At.Year() != 2026 || q.At.Hour() != 6 {
		t.Fatalf("at = %v", q.At)
	}
	q2, _ := Parse(`os.name=Debian at now`)
	if q2.At != nil {
		t.Fatalf("`at now` should be nil (now), got %v", q2.At)
	}
}

func TestParseErrors(t *testing.T) {
	for _, s := range []string{``, `role`, `=web`, `role where os.name=x`, `a where b=1 group by c`} {
		if _, err := Parse(s); err == nil {
			t.Errorf("expected error for %q", s)
		}
	}
	// project != group-by is explicitly unsupported in v1.
	if _, err := Parse(`certname where os.name=x group by role`); !errors.Is(err, ErrUnsupported) {
		t.Errorf("project!=groupby should be ErrUnsupported, got %v", err)
	}
}
