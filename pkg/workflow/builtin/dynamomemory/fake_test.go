package dynamomemory

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// fakeDynamo lets each test set just the methods it cares about. Unset methods
// return zero values, which keeps test bodies focused on the call paths under test.
type fakeDynamo struct {
	descTable     func(context.Context, *dynamodb.DescribeTableInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	createTable   func(context.Context, *dynamodb.CreateTableInput, ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
	descTTL       func(context.Context, *dynamodb.DescribeTimeToLiveInput, ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error)
	updateTTL     func(context.Context, *dynamodb.UpdateTimeToLiveInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateTimeToLiveOutput, error)
	query         func(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	transactWrite func(context.Context, *dynamodb.TransactWriteItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)

	descTableCalls     int
	createTableCalls   int
	descTTLCalls       int
	updateTTLCalls     int
	queryCalls         int
	transactWriteCalls int
}

func (f *fakeDynamo) DescribeTable(ctx context.Context, in *dynamodb.DescribeTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	f.descTableCalls++
	if f.descTable == nil {
		return &dynamodb.DescribeTableOutput{}, nil
	}
	return f.descTable(ctx, in, opts...)
}

func (f *fakeDynamo) CreateTable(ctx context.Context, in *dynamodb.CreateTableInput, opts ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	f.createTableCalls++
	if f.createTable == nil {
		return &dynamodb.CreateTableOutput{}, nil
	}
	return f.createTable(ctx, in, opts...)
}

func (f *fakeDynamo) DescribeTimeToLive(ctx context.Context, in *dynamodb.DescribeTimeToLiveInput, opts ...func(*dynamodb.Options)) (*dynamodb.DescribeTimeToLiveOutput, error) {
	f.descTTLCalls++
	if f.descTTL == nil {
		return &dynamodb.DescribeTimeToLiveOutput{}, nil
	}
	return f.descTTL(ctx, in, opts...)
}

func (f *fakeDynamo) UpdateTimeToLive(ctx context.Context, in *dynamodb.UpdateTimeToLiveInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateTimeToLiveOutput, error) {
	f.updateTTLCalls++
	if f.updateTTL == nil {
		return &dynamodb.UpdateTimeToLiveOutput{}, nil
	}
	return f.updateTTL(ctx, in, opts...)
}

func (f *fakeDynamo) Query(ctx context.Context, in *dynamodb.QueryInput, opts ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	f.queryCalls++
	if f.query == nil {
		return &dynamodb.QueryOutput{}, nil
	}
	return f.query(ctx, in, opts...)
}

func (f *fakeDynamo) TransactWriteItems(ctx context.Context, in *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	f.transactWriteCalls++
	if f.transactWrite == nil {
		return &dynamodb.TransactWriteItemsOutput{}, nil
	}
	return f.transactWrite(ctx, in, opts...)
}
