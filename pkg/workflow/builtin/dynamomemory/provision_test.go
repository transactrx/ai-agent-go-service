package dynamomemory

import (
	"context"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func newTestMem(client dynamoAPI) *dynamoMemory {
	return &dynamoMemory{
		cfg: Config{
			TableName: "test-table",
			Region:    "us-east-1",
			MaxTurns:  30,
			TTLDays:   90,
		},
		client: client,
	}
}

func quietLogger() *log.Logger { return log.New(io.Discard, "", 0) }

// shrinkPoll cuts the poll intervals so tests don't sleep for seconds.
func shrinkPoll(t *testing.T) {
	t.Helper()
	prevTI, prevTAB, prevLI, prevLB := tablePollInterval, tableActiveBudget, ttlPollInterval, ttlEnableBudget
	tablePollInterval = time.Millisecond
	tableActiveBudget = 100 * time.Millisecond
	ttlPollInterval = time.Millisecond
	ttlEnableBudget = 100 * time.Millisecond
	t.Cleanup(func() {
		tablePollInterval, tableActiveBudget, ttlPollInterval, ttlEnableBudget = prevTI, prevTAB, prevLI, prevLB
	})
}

func TestProvision_tableActive_ttlEnabled_noCalls(t *testing.T) {
	shrinkPoll(t)
	f := &fakeDynamo{
		descTable: func(ctx context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{TableStatus: types.TableStatusActive}}, nil
		},
		descTTL: func(ctx context.Context, _ *dynamodb.DescribeTimeToLiveInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error) {
			return &dynamodb.DescribeTimeToLiveOutput{
				TimeToLiveDescription: &types.TimeToLiveDescription{
					TimeToLiveStatus: types.TimeToLiveStatusEnabled,
					AttributeName:    aws.String(ttlAttribute),
				},
			}, nil
		},
	}
	m := newTestMem(f)
	if err := m.provision(context.Background(), quietLogger()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f.createTableCalls != 0 {
		t.Errorf("expected no CreateTable, got %d", f.createTableCalls)
	}
	if f.updateTTLCalls != 0 {
		t.Errorf("expected no UpdateTimeToLive, got %d", f.updateTTLCalls)
	}
}

func TestProvision_tableMissing_createsThenActive(t *testing.T) {
	shrinkPoll(t)
	callCount := 0
	f := &fakeDynamo{
		descTable: func(ctx context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			callCount++
			if callCount == 1 {
				return nil, &types.ResourceNotFoundException{}
			}
			return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{TableStatus: types.TableStatusActive}}, nil
		},
		descTTL: func(ctx context.Context, _ *dynamodb.DescribeTimeToLiveInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error) {
			return &dynamodb.DescribeTimeToLiveOutput{
				TimeToLiveDescription: &types.TimeToLiveDescription{
					TimeToLiveStatus: types.TimeToLiveStatusEnabled,
					AttributeName:    aws.String(ttlAttribute),
				},
			}, nil
		},
	}
	m := newTestMem(f)
	if err := m.provision(context.Background(), quietLogger()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f.createTableCalls != 1 {
		t.Errorf("expected exactly 1 CreateTable, got %d", f.createTableCalls)
	}
}

func TestProvision_createReturnsInUse_swallowed(t *testing.T) {
	shrinkPoll(t)
	notFoundFirst := true
	f := &fakeDynamo{
		descTable: func(ctx context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			if notFoundFirst {
				notFoundFirst = false
				return nil, &types.ResourceNotFoundException{}
			}
			return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{TableStatus: types.TableStatusActive}}, nil
		},
		createTable: func(ctx context.Context, _ *dynamodb.CreateTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
			return nil, &types.ResourceInUseException{}
		},
		descTTL: func(ctx context.Context, _ *dynamodb.DescribeTimeToLiveInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error) {
			return &dynamodb.DescribeTimeToLiveOutput{
				TimeToLiveDescription: &types.TimeToLiveDescription{
					TimeToLiveStatus: types.TimeToLiveStatusEnabled,
					AttributeName:    aws.String(ttlAttribute),
				},
			}, nil
		},
	}
	m := newTestMem(f)
	if err := m.provision(context.Background(), quietLogger()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f.createTableCalls != 1 {
		t.Errorf("expected exactly 1 CreateTable call, got %d", f.createTableCalls)
	}
}

func TestProvision_ttlDisabled_enables(t *testing.T) {
	shrinkPoll(t)
	ttlCount := 0
	f := &fakeDynamo{
		descTable: func(ctx context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{TableStatus: types.TableStatusActive}}, nil
		},
		descTTL: func(ctx context.Context, _ *dynamodb.DescribeTimeToLiveInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error) {
			ttlCount++
			if ttlCount == 1 {
				return &dynamodb.DescribeTimeToLiveOutput{
					TimeToLiveDescription: &types.TimeToLiveDescription{TimeToLiveStatus: types.TimeToLiveStatusDisabled},
				}, nil
			}
			return &dynamodb.DescribeTimeToLiveOutput{
				TimeToLiveDescription: &types.TimeToLiveDescription{
					TimeToLiveStatus: types.TimeToLiveStatusEnabled,
					AttributeName:    aws.String(ttlAttribute),
				},
			}, nil
		},
	}
	m := newTestMem(f)
	if err := m.provision(context.Background(), quietLogger()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if f.updateTTLCalls != 1 {
		t.Errorf("expected exactly 1 UpdateTimeToLive, got %d", f.updateTTLCalls)
	}
}

func TestProvision_ttlEnabledOnWrongAttribute_errors(t *testing.T) {
	shrinkPoll(t)
	f := &fakeDynamo{
		descTable: func(ctx context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
			return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{TableStatus: types.TableStatusActive}}, nil
		},
		descTTL: func(ctx context.Context, _ *dynamodb.DescribeTimeToLiveInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error) {
			return &dynamodb.DescribeTimeToLiveOutput{
				TimeToLiveDescription: &types.TimeToLiveDescription{
					TimeToLiveStatus: types.TimeToLiveStatusEnabled,
					AttributeName:    aws.String("ttl"),
				},
			}, nil
		},
	}
	m := newTestMem(f)
	err := m.provision(context.Background(), quietLogger())
	if err == nil || !strings.Contains(err.Error(), "TTL is ENABLED on attribute") {
		t.Fatalf("expected TTL-attribute mismatch error, got %v", err)
	}
}
