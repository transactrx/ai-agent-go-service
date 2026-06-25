package dynamomemory

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
	ttlPollInterval   = 2 * time.Second
	ttlEnableBudget   = 30 * time.Second
)

func (m *dynamoMemory) provision(ctx context.Context, lg *log.Logger) error {
	if err := m.ensureTable(ctx, lg); err != nil {
		return err
	}
	return m.ensureTTL(ctx, lg)
}

func (m *dynamoMemory) ensureTable(ctx context.Context, lg *log.Logger) error {
	desc, err := m.describeTable(ctx)
	if err == nil {
		return m.waitTableActive(ctx, desc.Table.TableStatus, lg)
	}
	var notFound *types.ResourceNotFoundException
	if !errors.As(err, &notFound) {
		return fmt.Errorf("memory/dynamodb: describe table: %w", err)
	}
	lg.Printf("memory/dynamodb: table %s missing — creating", m.cfg.TableName)
	if _, err := m.client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(m.cfg.TableName),
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
			return fmt.Errorf("memory/dynamodb: create table: %w", err)
		}
		lg.Printf("memory/dynamodb: table %s already being created by another booter", m.cfg.TableName)
	}
	return m.waitTableActive(ctx, types.TableStatusCreating, lg)
}

func (m *dynamoMemory) describeTable(ctx context.Context) (*dynamodb.DescribeTableOutput, error) {
	return m.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(m.cfg.TableName)})
}

func (m *dynamoMemory) waitTableActive(ctx context.Context, current types.TableStatus, lg *log.Logger) error {
	if current == types.TableStatusActive {
		lg.Printf("memory/dynamodb: table %s ACTIVE", m.cfg.TableName)
		return nil
	}
	deadline := time.Now().Add(tableActiveBudget)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(tablePollInterval):
		}
		desc, err := m.describeTable(ctx)
		if err != nil {
			return fmt.Errorf("memory/dynamodb: poll table: %w", err)
		}
		if desc.Table.TableStatus == types.TableStatusActive {
			lg.Printf("memory/dynamodb: table %s ACTIVE", m.cfg.TableName)
			return nil
		}
	}
	return fmt.Errorf("memory/dynamodb: table %s never reached ACTIVE within %s", m.cfg.TableName, tableActiveBudget)
}

func (m *dynamoMemory) ensureTTL(ctx context.Context, lg *log.Logger) error {
	out, err := m.client.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{
		TableName: aws.String(m.cfg.TableName),
	})
	if err != nil {
		return fmt.Errorf("memory/dynamodb: describe ttl: %w", err)
	}
	spec := out.TimeToLiveDescription
	if spec != nil && spec.TimeToLiveStatus == types.TimeToLiveStatusEnabled {
		if aws.ToString(spec.AttributeName) == ttlAttribute {
			lg.Printf("memory/dynamodb: TTL ENABLED on %s.%s", m.cfg.TableName, ttlAttribute)
			return nil
		}
		return fmt.Errorf("memory/dynamodb: TTL is ENABLED on attribute %q, expected %q — refusing to silently change another consumer's config", aws.ToString(spec.AttributeName), ttlAttribute)
	}
	if spec != nil && spec.TimeToLiveStatus == types.TimeToLiveStatusEnabling {
		return m.waitTTLEnabled(ctx, lg)
	}
	lg.Printf("memory/dynamodb: enabling TTL on %s.%s", m.cfg.TableName, ttlAttribute)
	if _, err := m.client.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String(m.cfg.TableName),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			AttributeName: aws.String(ttlAttribute),
			Enabled:       aws.Bool(true),
		},
	}); err != nil {
		var inUse *types.ResourceInUseException
		if !errors.As(err, &inUse) {
			return fmt.Errorf("memory/dynamodb: update ttl: %w", err)
		}
	}
	return m.waitTTLEnabled(ctx, lg)
}

func (m *dynamoMemory) waitTTLEnabled(ctx context.Context, lg *log.Logger) error {
	deadline := time.Now().Add(ttlEnableBudget)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(ttlPollInterval):
		}
		out, err := m.client.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{
			TableName: aws.String(m.cfg.TableName),
		})
		if err != nil {
			return fmt.Errorf("memory/dynamodb: poll ttl: %w", err)
		}
		if out.TimeToLiveDescription != nil && out.TimeToLiveDescription.TimeToLiveStatus == types.TimeToLiveStatusEnabled {
			lg.Printf("memory/dynamodb: TTL ENABLED on %s.%s", m.cfg.TableName, ttlAttribute)
			return nil
		}
	}
	return fmt.Errorf("memory/dynamodb: TTL never reached ENABLED within %s", ttlEnableBudget)
}
