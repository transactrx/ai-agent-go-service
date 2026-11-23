package clientui_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/transactrx/nats-agent/pkg/ask"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/builtin/clientui"
)

// Round trip: a realistic PowerLine input (shapes the model sends per each
// tool's InputSchema, powerlineSearch.json uiAsk* nodes) → ask.Ask (must pass
// ask.CheckAsk) → a fixture submit (must pass ask.Validate) → the legacy
// result shape the workflow loop already understands.
func TestTranslateRoundTrip(t *testing.T) {
	cases := []struct {
		name     string
		nodeType string
		input    string
		check    func(t *testing.T, a ask.Ask)
		values   string // submitted values (wire shape the RSAssistant form sends)
		want     string // legacy result JSON
	}{
		{
			name: "ConfirmAction", nodeType: "tool/ui-confirm",
			input: `{"prompt":"Resubmit 12 rejected claims for pharmacy 1234567?"}`,
			check: func(t *testing.T, a ask.Ask) {
				if a.Mode != ask.ModeConfirm || len(a.Fields) != 0 || a.Title != "Resubmit 12 rejected claims for pharmacy 1234567?" {
					t.Fatalf("ask = %+v", a)
				}
			},
			values: `{}`,
			want:   `{"approved":true}`,
		},
		{
			name: "PickOne", nodeType: "tool/ui-pick-one",
			input: `{"prompt":"Which pharmacy did you mean?","options":[{"value":"1234567","label":"Main St Pharmacy (NCPDP 1234567)"},{"value":"7654321","label":"Main St Pharmacy #2 (NCPDP 7654321)"}]}`,
			check: func(t *testing.T, a ask.Ask) {
				f := a.Fields[0]
				if f.Type != ask.TypeChoice || f.Multiple || f.AllowOther == nil || *f.AllowOther || len(f.Options) != 2 || !f.Required {
					t.Fatalf("field = %+v", f)
				}
				if f.Options[1].Label != "Main St Pharmacy #2 (NCPDP 7654321)" {
					t.Fatalf("option label = %q", f.Options[1].Label)
				}
			},
			values: `{"value":"7654321"}`,
			want:   `{"value":"7654321"}`,
		},
		{
			name: "PickMany", nodeType: "tool/ui-pick-many",
			input: `{"prompt":"Which switches should I include?","options":[{"value":"emdeon","label":"Emdeon"},{"value":"relayhealth","label":"RelayHealth"},{"value":"surescripts","label":"Surescripts"}],"minPicks":1,"maxPicks":2}`,
			check: func(t *testing.T, a ask.Ask) {
				f := a.Fields[0]
				if f.Type != ask.TypeChoice || !f.Multiple || f.MinItems != 1 || f.MaxItems != 2 || *f.AllowOther {
					t.Fatalf("field = %+v", f)
				}
			},
			values: `{"values":["emdeon","surescripts"]}`,
			want:   `{"values":["emdeon","surescripts"]}`,
		},
		{
			name: "AskUser", nodeType: "tool/ui-human-input",
			input: `{"prompt":"What BIN/PCN should I filter on?","placeholder":"e.g. 610014 / MEDDPRIME"}`,
			check: func(t *testing.T, a ask.Ask) {
				f := a.Fields[0]
				if f.Type != ask.TypeText || f.Placeholder != "e.g. 610014 / MEDDPRIME" || !f.Required {
					t.Fatalf("field = %+v", f)
				}
			},
			values: `{"text":"610014 / MEDDPRIME"}`,
			want:   `{"text":"610014 / MEDDPRIME"}`,
		},
		{
			name: "AskLongText", nodeType: "tool/ui-ask-long-text",
			input: `{"prompt":"Paste the rejection log you are seeing","placeholder":"raw log lines","minLength":10,"maxLength":4000}`,
			check: func(t *testing.T, a ask.Ask) {
				f := a.Fields[0]
				if f.Type != ask.TypeLongText || f.MinLength != 10 || f.MaxLength != 4000 {
					t.Fatalf("field = %+v", f)
				}
			},
			values: `{"text":"line 1: reject 75\nline 2: reject 79"}`,
			want:   `{"text":"line 1: reject 75\nline 2: reject 79"}`,
		},
		{
			name: "AskDate single", nodeType: "tool/ui-ask-date",
			input: `{"prompt":"Which fill date?","mode":"single","min":"2026-01-01","max":"2026-11-22"}`,
			check: func(t *testing.T, a ask.Ask) {
				f := a.Fields[0]
				if f.Type != ask.TypeDate || f.Min != "2026-01-01" || f.Max != "2026-11-22" {
					t.Fatalf("field = %+v", f)
				}
			},
			values: `{"date":"2026-11-22"}`,
			want:   `{"date":"2026-11-22"}`,
		},
		{
			name: "AskDate range", nodeType: "tool/ui-ask-date",
			input: `{"prompt":"Which period?","mode":"range"}`,
			check: func(t *testing.T, a ask.Ask) {
				if f := a.Fields[0]; f.Type != ask.TypeDateRange {
					t.Fatalf("field = %+v", f)
				}
			},
			values: `{"range":{"from":"2026-11-01","to":"2026-11-22"}}`,
			want:   `{"from":"2026-11-01","to":"2026-11-22"}`,
		},
		{
			name: "AskNumber slider", nodeType: "tool/ui-ask-number",
			input: `{"prompt":"How many days back?","mode":"slider","min":1,"max":90,"step":1}`,
			check: func(t *testing.T, a ask.Ask) {
				f := a.Fields[0]
				if f.Type != ask.TypeNumber || f.Display != "slider" || f.Min != 1.0 || f.Max != 90.0 || f.Step != 1 {
					t.Fatalf("field = %+v", f)
				}
			},
			values: `{"value":30}`,
			want:   `{"value":30}`,
		},
		{
			name: "AskNumber input", nodeType: "tool/ui-ask-number",
			input: `{"prompt":"Minimum claim amount?","mode":"input"}`,
			check: func(t *testing.T, a ask.Ask) {
				if f := a.Fields[0]; f.Type != ask.TypeNumber || f.Display != "" {
					t.Fatalf("field = %+v", f)
				}
			},
			values: `{"value":12.5}`,
			want:   `{"value":12.5}`,
		},
		{
			name: "PickRow", nodeType: "tool/ui-pick-row",
			input: `{"prompt":"Which claim?","columns":[{"id":"rx","label":"Rx #","width":80},{"id":"drug","label":"Drug"}],"rows":[{"id":"c1","rx":"100234","drug":"Atorvastatin 20mg"},{"id":"c2","rx":"100235","drug":"Lisinopril 10mg"}]}`,
			check: func(t *testing.T, a ask.Ask) {
				f := a.Fields[0]
				if f.Type != ask.TypeTable || len(f.Columns) != 2 || len(f.Rows) != 2 || f.MaxItems != 1 || f.Columns[0].Label != "Rx #" {
					t.Fatalf("field = %+v", f)
				}
			},
			values: `{"row":{"id":"c2","rx":"100235","drug":"Lisinopril 10mg"}}`,
			want:   `{"row":{"drug":"Lisinopril 10mg","id":"c2","rx":"100235"}}`,
		},
		{
			name: "AskForm", nodeType: "tool/ui-ask-form",
			input: `{"prompt":"Claim search filters","fields":[
				{"name":"ncpdp","label":"NCPDP","type":"text","required":true,"placeholder":"7 digits"},
				{"name":"status","label":"Status","type":"select","options":[{"value":"paid","label":"Paid"},{"value":"rejected","label":"Rejected"}]},
				{"name":"from","label":"From","type":"date"},
				{"name":"minAmount","label":"Min amount","type":"number","min":0,"max":10000},
				{"name":"reversals","label":"Include reversals","type":"checkbox"}]}`,
			check: func(t *testing.T, a ask.Ask) {
				got := []string{}
				for _, f := range a.Fields {
					got = append(got, f.Name+":"+f.Type)
				}
				if strings.Join(got, ",") != "ncpdp:text,status:choice,from:date,minAmount:number,reversals:checkbox" {
					t.Fatalf("fields = %v", got)
				}
				if !a.Fields[0].Required || a.Fields[0].Placeholder != "7 digits" || *a.Fields[1].AllowOther || a.Fields[3].Max != 10000.0 {
					t.Fatalf("fields = %+v", a.Fields)
				}
			},
			values: `{"ncpdp":"1234567","status":"rejected","from":"2026-11-01","minAmount":50}`,
			want:   `{"from":"2026-11-01","minAmount":50,"ncpdp":"1234567","reversals":false,"status":"rejected"}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var in map[string]any
			if err := json.Unmarshal([]byte(tc.input), &in); err != nil {
				t.Fatal(err)
			}
			a, back, err := clientui.Translate(tc.nodeType, in)
			if err != nil {
				t.Fatalf("Translate: %v", err)
			}
			if err := ask.CheckAsk(a); err != nil {
				t.Fatalf("CheckAsk: %v", err)
			}
			tc.check(t, a)
			var vals map[string]any
			if err := json.Unmarshal([]byte(tc.values), &vals); err != nil {
				t.Fatal(err)
			}
			if errs := ask.Validate(a, vals); len(errs) > 0 {
				t.Fatalf("Validate: %+v", errs)
			}
			got, _ := json.Marshal(back(ask.BuildAnswer(a, "ask_1", vals)))
			if !jsonEqual(t, string(got), tc.want) {
				t.Fatalf("result = %s, want %s", got, tc.want)
			}
			// Skip keeps the existing "user declined" path, except a declined
			// confirmation, which is the legacy rejection.
			skipped, _ := json.Marshal(back(ask.Answer{AskID: "ask_1", Status: ask.StatusSkipped}))
			wantSkip := `{"_skipped":true}`
			if tc.nodeType == "tool/ui-confirm" {
				wantSkip = `{"approved":false}`
			}
			if string(skipped) != wantSkip {
				t.Fatalf("skip result = %s, want %s", skipped, wantSkip)
			}
		})
	}
}

func TestTranslateRejectsUnknownTool(t *testing.T) {
	if _, _, err := clientui.Translate("tool/opensearch", map[string]any{"prompt": "x"}); err == nil {
		t.Fatal("unknown node type must error")
	}
}

// Limits from the ask schema surface as errors before any card is shown.
func TestTranslateOverLimitFailsCheck(t *testing.T) {
	rows := []any{}
	for i := 0; i < ask.MaxRows+1; i++ {
		rows = append(rows, map[string]any{"id": string(rune('a'+i%26)) + string(rune('a'+i/26))})
	}
	a, _, err := clientui.Translate("tool/ui-pick-row", map[string]any{
		"prompt": "Which?", "columns": []any{map[string]any{"id": "id", "label": "Id"}}, "rows": rows,
	})
	if err == nil {
		err = ask.CheckAsk(a)
	}
	if err == nil || !strings.Contains(err.Error(), "too many rows") {
		t.Fatalf("err = %v", err)
	}
}

// A long prompt becomes a short title plus the full text as description.
func TestTranslateLongPrompt(t *testing.T) {
	long := strings.Repeat("Please confirm the reprocessing of these claims. ", 6)
	a, _, err := clientui.Translate("tool/ui-confirm", map[string]any{"prompt": long})
	if err != nil {
		t.Fatal(err)
	}
	if err := ask.CheckAsk(a); err != nil {
		t.Fatalf("CheckAsk: %v", err)
	}
	if a.Description != strings.TrimSpace(long) {
		t.Fatalf("description = %q", a.Description)
	}
}

func jsonEqual(t *testing.T, a, b string) bool {
	t.Helper()
	var x, y any
	if err := json.Unmarshal([]byte(a), &x); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(b), &y); err != nil {
		t.Fatal(err)
	}
	return reflect.DeepEqual(x, y)
}

// An empty prompt gets a default title so the ask passes CheckAsk.
func TestTranslateEmptyPromptDefaultsTitle(t *testing.T) {
	for _, c := range []struct{ nodeType, input, want string }{
		{"tool/ui-confirm", `{}`, "Please confirm"},
		{"tool/ui-confirm", `{"prompt":"   "}`, "Please confirm"},
		{"tool/ui-human-input", `{}`, "A few details"},
		{"tool/ui-pick-one", `{"options":[{"value":"a","label":"A"},{"value":"b","label":"B"}]}`, "A few details"},
	} {
		var in map[string]any
		_ = json.Unmarshal([]byte(c.input), &in)
		a, _, err := clientui.Translate(c.nodeType, in)
		if err != nil {
			t.Fatal(err)
		}
		if a.Title != c.want {
			t.Fatalf("%s %s: title = %q, want %q", c.nodeType, c.input, a.Title, c.want)
		}
		if err := ask.CheckAsk(a); err != nil {
			t.Fatalf("%s: CheckAsk: %v", c.nodeType, err)
		}
	}
}

// A superseded ask (the user wrote instead of answering) maps to the text
// slot where the tool has one, the legacy decline otherwise.
func TestSupersededResult(t *testing.T) {
	for _, c := range []struct{ nodeType, want string }{
		{"tool/ui-human-input", `{"text":"use last week"}`},
		{"tool/ui-ask-long-text", `{"text":"use last week"}`},
		{"tool/ui-confirm", `{"approved":false}`},
		{"tool/ui-pick-one", `{"_skipped":true}`},
		{"tool/ui-ask-form", `{"_skipped":true}`},
	} {
		got, _ := json.Marshal(clientui.SupersededResult(c.nodeType, "use last week"))
		if string(got) != c.want {
			t.Fatalf("%s: %s, want %s", c.nodeType, got, c.want)
		}
	}
}
