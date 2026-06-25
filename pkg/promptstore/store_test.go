package promptstore

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// onceFailDynamo delegates to fakeDynamo but returns ConditionalCheckFailedException
// on the very first PutItem call.
type onceFailDynamo struct {
	inner *fakeDynamo
	calls int
}

func (o *onceFailDynamo) PutItem(ctx context.Context, in *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	o.calls++
	if o.calls == 1 {
		return nil, &types.ConditionalCheckFailedException{}
	}
	return o.inner.PutItem(ctx, in, opts...)
}

func (o *onceFailDynamo) Query(ctx context.Context, in *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	return o.inner.Query(ctx, in, opts...)
}

func (o *onceFailDynamo) DescribeTable(ctx context.Context, in *dynamodb.DescribeTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	return o.inner.DescribeTable(ctx, in, opts...)
}

func (o *onceFailDynamo) CreateTable(ctx context.Context, in *dynamodb.CreateTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	return o.inner.CreateTable(ctx, in, opts...)
}

// alwaysFailDynamo always returns ConditionalCheckFailedException.
type alwaysFailDynamo struct {
	calls int
}

func (a *alwaysFailDynamo) PutItem(_ context.Context, _ *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	a.calls++
	return nil, &types.ConditionalCheckFailedException{}
}

func (a *alwaysFailDynamo) Query(_ context.Context, _ *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	return &dynamodb.QueryOutput{}, nil
}

func (a *alwaysFailDynamo) DescribeTable(_ context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	return &dynamodb.DescribeTableOutput{}, nil
}

func (a *alwaysFailDynamo) CreateTable(_ context.Context, _ *dynamodb.CreateTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	return &dynamodb.CreateTableOutput{}, nil
}

func newTestStore(f *fakeDynamo) *Store {
	return &Store{client: f, table: "opensearchaichatapi-assistant-prompts"}
}

func TestLatest_EmptyTable(t *testing.T) {
	st := newTestStore(newFakeDynamo())
	_, found, err := st.Latest(context.Background(), "powerlineSearch", "agent1", "systemMessageFlexible")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if found {
		t.Fatal("expected not found on empty table")
	}
}

func TestSaveThenLatest(t *testing.T) {
	st := newTestStore(newFakeDynamo())
	ctx := context.Background()
	if _, err := st.Save(ctx, "powerlineSearch", "agent1", "systemMessageFlexible", "v1 text", "user-a"); err != nil {
		t.Fatalf("save1: %v", err)
	}
	if _, err := st.Save(ctx, "powerlineSearch", "agent1", "systemMessageFlexible", "v2 text", "user-b"); err != nil {
		t.Fatalf("save2: %v", err)
	}
	got, found, err := st.Latest(ctx, "powerlineSearch", "agent1", "systemMessageFlexible")
	if err != nil || !found {
		t.Fatalf("latest: found=%v err=%v", found, err)
	}
	if got != "v2 text" {
		t.Fatalf("latest = %q, want v2 text", got)
	}
}

func TestHistory_NewestFirstAndAudit(t *testing.T) {
	st := newTestStore(newFakeDynamo())
	ctx := context.Background()
	st.Save(ctx, "wf", "n", "f", "first", "alice")
	st.Save(ctx, "wf", "n", "f", "second", "bob")
	hist, err := st.History(ctx, "wf", "n", "f", 10)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(hist) != 2 {
		t.Fatalf("len = %d, want 2", len(hist))
	}
	if hist[0].Content != "second" || hist[0].SavedBy != "bob" {
		t.Fatalf("newest = %+v, want second/bob", hist[0])
	}
	if hist[1].Content != "first" || hist[1].SavedBy != "alice" {
		t.Fatalf("oldest = %+v, want first/alice", hist[1])
	}
}

func TestSave_RetriesOnSameMsCollision(t *testing.T) {
	fake := newFakeDynamo()
	st := &Store{client: &onceFailDynamo{inner: fake}, table: "tbl"}
	ver, err := st.Save(context.Background(), "wf", "n", "f", "content", "user")
	if err != nil {
		t.Fatalf("expected success after retry, got: %v", err)
	}
	if ver == "" {
		t.Fatal("expected non-empty version")
	}
}

func TestSave_ExhaustsRetries(t *testing.T) {
	afd := &alwaysFailDynamo{}
	st := &Store{client: afd, table: "tbl"}
	_, err := st.Save(context.Background(), "wf", "n", "f", "content", "user")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "after 5 retries") {
		t.Fatalf("expected 'after 5 retries' in error, got: %v", err)
	}
}

func TestSave_NonConditionalErrorSurfaces(t *testing.T) {
	fake := newFakeDynamo()
	fake.putErr = errors.New("boom")
	st := newTestStore(fake)
	_, err := st.Save(context.Background(), "wf", "n", "f", "content", "user")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected 'boom' in error, got: %v", err)
	}
}
