package clientui

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/transactrx/nats-agent/pkg/ask"
)

// Translate turns one client-UI tool call into a nats-agent ask (protocol
// 1.7 "ask the user"), for runs where the caller answers asks instead of
// rendering the Webix widget. nodeType is the tool's node type (e.g.
// "tool/ui-pick-one"): tool names are workflow config, node types are fixed.
// input is the tool call's input, shaped by the tool's InputSchema.
//
// back maps the answer onto the legacy result the agent loop expects from
// the Webix widget ({approved}, {value}, {values}, {text}, {date},
// {from,to}, {row}, the form object). A skipped answer becomes
// {"_skipped":true} (the loop's "user declined" path), except a declined
// confirmation, which is {"approved":false}.
//
// The ask is not checked here; callers run ask.CheckAsk (Stream.AskUser
// does) and report its error to the model.
func Translate(nodeType string, input map[string]any) (ask.Ask, func(ask.Answer) map[string]any, error) {
	t, ok := translators[nodeType]
	if !ok {
		return ask.Ask{}, nil, fmt.Errorf("clientui: %q is not a client UI tool", nodeType)
	}
	a, from, err := t(input)
	if err != nil {
		return ask.Ask{}, nil, err
	}
	confirm := nodeType == "tool/ui-confirm"
	setTitle(&a, str(input, "prompt"), confirm)
	back := func(ans ask.Answer) map[string]any {
		if ans.Status != ask.StatusSubmitted && !confirm {
			return map[string]any{"_skipped": true}
		}
		return from(ans)
	}
	return a, back, nil
}

// SupersededResult is the legacy result for a client-UI ask the user
// superseded (wrote a message instead of answering): the user's text in the
// tool's text slot (ui-human-input, ui-ask-long-text), {"approved":false}
// for a confirmation, else {"_skipped":true} (the loop's "user declined"
// path). The model sees the message itself in the next turn either way.
func SupersededResult(nodeType, text string) map[string]any {
	switch nodeType {
	case "tool/ui-human-input", "tool/ui-ask-long-text":
		return map[string]any{"text": text}
	case "tool/ui-confirm":
		return map[string]any{"approved": false}
	}
	return map[string]any{"_skipped": true}
}

type translator func(input map[string]any) (ask.Ask, func(ask.Answer) map[string]any, error)

var translators = map[string]translator{
	"tool/ui-confirm":       confirmAsk,
	"tool/ui-pick-one":      pickOneAsk,
	"tool/ui-pick-many":     pickManyAsk,
	"tool/ui-human-input":   humanInputAsk,
	"tool/ui-ask-long-text": longTextAsk,
	"tool/ui-ask-date":      dateAsk,
	"tool/ui-ask-number":    numberAsk,
	"tool/ui-ask-form":      formAsk,
	"tool/ui-pick-row":      pickRowAsk,
}

var noOther = func() *bool { b := false; return &b }

// Default titles for a call without a prompt (an ask needs a title).
const (
	defaultConfirmTitle = "Please confirm"
	defaultTitle        = "A few details"
)

// setTitle puts the prompt in the title; a prompt over the title limit goes
// whole into the description (cut to its limit) under a shortened title. No
// prompt: a default title.
func setTitle(a *ask.Ask, prompt string, confirm bool) {
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		a.Title = defaultTitle
		if confirm {
			a.Title = defaultConfirmTitle
		}
		return
	}
	if utf8.RuneCountInString(prompt) <= ask.MaxTitle {
		a.Title = prompt
		return
	}
	a.Title = cut(prompt, ask.MaxTitle)
	a.Description = cut(prompt, ask.MaxDescription)
}

func cut(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return strings.TrimSpace(string(r[:n-1])) + "…"
}

func confirmAsk(map[string]any) (ask.Ask, func(ask.Answer) map[string]any, error) {
	return ask.Ask{Mode: ask.ModeConfirm}, func(ans ask.Answer) map[string]any {
		return map[string]any{"approved": ans.Status == ask.StatusSubmitted}
	}, nil
}

func pickOneAsk(in map[string]any) (ask.Ask, func(ask.Answer) map[string]any, error) {
	f := ask.Field{Name: "value", Type: ask.TypeChoice, Required: true, AllowOther: noOther(), Options: options(in["options"])}
	return ask.Ask{Fields: []ask.Field{f}}, func(ans ask.Answer) map[string]any {
		return map[string]any{"value": ans.Values["value"]}
	}, nil
}

func pickManyAsk(in map[string]any) (ask.Ask, func(ask.Answer) map[string]any, error) {
	opts := options(in["options"])
	minPicks := 1 // tool default
	if v, ok := num(in, "minPicks"); ok {
		minPicks = int(v)
	}
	maxPicks := 0 // 0 = all options (tool default)
	if v, ok := num(in, "maxPicks"); ok {
		maxPicks = int(v)
	}
	f := ask.Field{Name: "values", Type: ask.TypeChoice, Multiple: true, AllowOther: noOther(), Options: opts,
		Required: minPicks > 0, MinItems: minPicks, MaxItems: maxPicks}
	return ask.Ask{Fields: []ask.Field{f}}, func(ans ask.Answer) map[string]any {
		vals, _ := ans.Values["values"].([]any)
		if vals == nil {
			vals = []any{}
		}
		return map[string]any{"values": vals}
	}, nil
}

func humanInputAsk(in map[string]any) (ask.Ask, func(ask.Answer) map[string]any, error) {
	f := ask.Field{Name: "text", Type: ask.TypeText, Required: true, Placeholder: str(in, "placeholder")}
	return ask.Ask{Fields: []ask.Field{f}}, textBack, nil
}

func longTextAsk(in map[string]any) (ask.Ask, func(ask.Answer) map[string]any, error) {
	f := ask.Field{Name: "text", Type: ask.TypeLongText, Required: true, Placeholder: str(in, "placeholder")}
	if v, ok := num(in, "minLength"); ok {
		f.MinLength = int(v)
	}
	if v, ok := num(in, "maxLength"); ok {
		f.MaxLength = int(v)
	}
	return ask.Ask{Fields: []ask.Field{f}}, textBack, nil
}

func textBack(ans ask.Answer) map[string]any {
	s, _ := ans.Values["text"].(string)
	return map[string]any{"text": s}
}

func dateAsk(in map[string]any) (ask.Ask, func(ask.Answer) map[string]any, error) {
	f := ask.Field{Required: true}
	if s := str(in, "min"); s != "" {
		f.Min = s
	}
	if s := str(in, "max"); s != "" {
		f.Max = s
	}
	if str(in, "mode") == "range" {
		f.Name, f.Type = "range", ask.TypeDateRange
		return ask.Ask{Fields: []ask.Field{f}}, func(ans ask.Answer) map[string]any {
			m, _ := ans.Values["range"].(map[string]any)
			return map[string]any{"from": m["from"], "to": m["to"]}
		}, nil
	}
	f.Name, f.Type = "date", ask.TypeDate
	return ask.Ask{Fields: []ask.Field{f}}, func(ans ask.Answer) map[string]any {
		return map[string]any{"date": ans.Values["date"]}
	}, nil
}

func numberAsk(in map[string]any) (ask.Ask, func(ask.Answer) map[string]any, error) {
	f := ask.Field{Name: "value", Type: ask.TypeNumber, Required: true}
	if str(in, "mode") == "slider" {
		f.Display = "slider"
	}
	if v, ok := num(in, "min"); ok {
		f.Min = v
	}
	if v, ok := num(in, "max"); ok {
		f.Max = v
	}
	if v, ok := num(in, "step"); ok && v > 0 {
		f.Step = v
	}
	return ask.Ask{Fields: []ask.Field{f}}, func(ans ask.Answer) map[string]any {
		return map[string]any{"value": ans.Values["value"]}
	}, nil
}

func pickRowAsk(in map[string]any) (ask.Ask, func(ask.Answer) map[string]any, error) {
	f := ask.Field{Name: "row", Type: ask.TypeTable, Required: true, MaxItems: 1}
	cols, _ := in["columns"].([]any)
	for _, c := range cols {
		m, _ := c.(map[string]any)
		f.Columns = append(f.Columns, ask.Column{ID: str(m, "id"), Label: str(m, "label")})
	}
	rows, _ := in["rows"].([]any)
	for _, r := range rows {
		m, ok := r.(map[string]any)
		if !ok {
			return ask.Ask{}, nil, fmt.Errorf("rows must be objects")
		}
		f.Rows = append(f.Rows, m)
	}
	return ask.Ask{Fields: []ask.Field{f}}, func(ans ask.Answer) map[string]any {
		row := ans.Values["row"]
		if l, ok := row.([]any); ok && len(l) > 0 {
			row = l[0]
		}
		return map[string]any{"row": row}
	}, nil
}

// formTypes maps AskForm field types one to one.
var formTypes = map[string]string{
	"text": ask.TypeText, "select": ask.TypeChoice, "date": ask.TypeDate,
	"number": ask.TypeNumber, "checkbox": ask.TypeCheckbox,
}

func formAsk(in map[string]any) (ask.Ask, func(ask.Answer) map[string]any, error) {
	var a ask.Ask
	var checkboxes []string
	fields, _ := in["fields"].([]any)
	for _, raw := range fields {
		m, _ := raw.(map[string]any)
		typ, ok := formTypes[str(m, "type")]
		if !ok {
			return ask.Ask{}, nil, fmt.Errorf("field %q: unknown type %q (use text, select, date, number or checkbox)", str(m, "name"), str(m, "type"))
		}
		f := ask.Field{Name: str(m, "name"), Label: str(m, "label"), Type: typ}
		f.Required, _ = m["required"].(bool)
		switch typ {
		case ask.TypeChoice:
			f.Options, f.AllowOther = options(m["options"]), noOther()
		case ask.TypeNumber:
			if v, ok := num(m, "min"); ok {
				f.Min = v
			}
			if v, ok := num(m, "max"); ok {
				f.Max = v
			}
			f.Placeholder = str(m, "placeholder")
		case ask.TypeText:
			f.Placeholder = str(m, "placeholder")
		case ask.TypeCheckbox:
			checkboxes = append(checkboxes, f.Name)
		}
		a.Fields = append(a.Fields, f)
	}
	return a, func(ans ask.Answer) map[string]any {
		out := map[string]any{}
		for k, v := range ans.Values {
			out[k] = v
		}
		for _, n := range checkboxes { // an unticked box is false, as in Webix
			if _, ok := out[n]; !ok {
				out[n] = false
			}
		}
		return out
	}, nil
}

func options(v any) []ask.Option {
	list, _ := v.([]any)
	out := make([]ask.Option, 0, len(list))
	for _, o := range list {
		m, _ := o.(map[string]any)
		out = append(out, ask.Option{Value: str(m, "value"), Label: str(m, "label")})
	}
	return out
}

func str(m map[string]any, k string) string {
	s, _ := m[k].(string)
	return s
}

func num(m map[string]any, k string) (float64, bool) {
	switch v := m[k].(type) {
	case float64:
		return v, true
	case int:
		return float64(v), true
	}
	return 0, false
}
