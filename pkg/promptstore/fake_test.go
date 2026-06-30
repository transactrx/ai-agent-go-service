package promptstore

import (
	"context"
	"sort"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// fakeDynamo stores items in-memory keyed pk -> sk -> attributes.
type fakeDynamo struct {
	items     map[string]map[string]map[string]types.AttributeValue
	putErr    error
	queryErr  error
	tableSeen string
}

func newFakeDynamo() *fakeDynamo {
	return &fakeDynamo{items: map[string]map[string]map[string]types.AttributeValue{}}
}

func s(v types.AttributeValue) string {
	if m, ok := v.(*types.AttributeValueMemberS); ok {
		return m.Value
	}
	return ""
}

func (f *fakeDynamo) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if f.putErr != nil {
		return nil, f.putErr
	}
	f.tableSeen = aws.ToString(in.TableName)
	pk, sk := s(in.Item["pk"]), s(in.Item["sk"])
	if f.items[pk] == nil {
		f.items[pk] = map[string]map[string]types.AttributeValue{}
	}
	if _, dup := f.items[pk][sk]; dup && in.ConditionExpression != nil {
		return nil, &types.ConditionalCheckFailedException{Message: aws.String("exists")}
	}
	f.items[pk][sk] = in.Item
	return &dynamodb.PutItemOutput{}, nil
}

func (f *fakeDynamo) Query(_ context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	pk := s(in.ExpressionAttributeValues[":pk"])
	var sks []string
	for sk := range f.items[pk] {
		sks = append(sks, sk)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(sks))) // ScanIndexForward=false
	limit := len(sks)
	if in.Limit != nil && int(*in.Limit) < limit {
		limit = int(*in.Limit)
	}
	out := &dynamodb.QueryOutput{}
	for _, sk := range sks[:limit] {
		out.Items = append(out.Items, f.items[pk][sk])
	}
	return out, nil
}

func (f *fakeDynamo) DescribeTable(_ context.Context, _ *dynamodb.DescribeTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	return &dynamodb.DescribeTableOutput{Table: &types.TableDescription{TableStatus: types.TableStatusActive}}, nil
}

func (f *fakeDynamo) CreateTable(_ context.Context, _ *dynamodb.CreateTableInput, _ ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error) {
	return &dynamodb.CreateTableOutput{}, nil
}
