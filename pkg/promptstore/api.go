// Package promptstore persists admin-editable prompt overrides, versioned,
// in one generic DynamoDB table (pk = workflowId#nodeId#field, sk = v#millis).
package promptstore

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// dynamoAPI is the slice of *dynamodb.Client this package uses. Tests inject
// a fake; the real client satisfies it for free.
type dynamoAPI interface {
	DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	CreateTable(ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
	Query(ctx context.Context, params *dynamodb.QueryInput, optFns ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
}
