package promptstore

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Poll intervals are package vars so tests can shrink them.
var (
	tablePollInterval = 2 * time.Second
	tableActiveBudget = 60 * time.Second
)

// Provision ensures the table exists and is ACTIVE. No TTL: versions are the
// audit trail and must not expire.
func (s *Store) Provision(ctx context.Context, lg *log.Logger) error {
	desc, err := s.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(s.table)})
	if err == nil {
		return s.waitActive(ctx, desc.Table.TableStatus, lg)
	}
	var notFound *types.ResourceNotFoundException
	if !errors.As(err, &notFound) {
		return fmt.Errorf("promptstore: describe table: %w", err)
	}
	lg.Printf("promptstore: table %s missing — creating", s.table)
	if _, err := s.client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(s.table),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		BillingMode: types.BillingModePayPerRequest,
	}); err != nil {
		var inUse *types.ResourceInUseException
		if !errors.As(err, &inUse) {
			return fmt.Errorf("promptstore: create table: %w", err)
		}
	}
	return s.waitActive(ctx, types.TableStatusCreating, lg)
}

func (s *Store) waitActive(ctx context.Context, current types.TableStatus, lg *log.Logger) error {
	if current == types.TableStatusActive {
		lg.Printf("promptstore: table %s ACTIVE", s.table)
		return nil
	}
	deadline := time.Now().Add(tableActiveBudget)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(tablePollInterval):
		}
		desc, err := s.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(s.table)})
		if err != nil {
			return fmt.Errorf("promptstore: poll table: %w", err)
		}
		if desc.Table.TableStatus == types.TableStatusActive {
			lg.Printf("promptstore: table %s ACTIVE", s.table)
			return nil
		}
	}
	return fmt.Errorf("promptstore: table %s never reached ACTIVE within %s", s.table, tableActiveBudget)
}
