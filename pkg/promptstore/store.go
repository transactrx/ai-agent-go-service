package promptstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const maxSaveRetries = 5

// Version is one stored revision of a prompt field.
type Version struct {
	Version   string // sk, e.g. "v#0001717500000000"
	Content   string // RAW text, ${ENV} unresolved
	SavedBy   string // X-User-Id of the saver
	CreatedAt time.Time
}

// Store is the DynamoDB-backed prompt-override store. One item per saved
// version; Latest = highest sk.
type Store struct {
	client dynamoAPI
	table  string
}

// New builds a Store with a real DynamoDB client.
func New(ctx context.Context, table, region string) (*Store, error) {
	if table == "" {
		return nil, fmt.Errorf("promptstore: table name required")
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("promptstore: load aws config: %w", err)
	}
	return &Store{client: dynamodb.NewFromConfig(awsCfg), table: table}, nil
}

func buildPK(workflowID, nodeID, field string) string {
	return workflowID + "#" + nodeID + "#" + field
}

// Latest returns the most recent content for the key, found=false when no
// version exists.
func (s *Store) Latest(ctx context.Context, workflowID, nodeID, field string) (string, bool, error) {
	out, err := s.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(s.table),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: buildPK(workflowID, nodeID, field)},
		},
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int32(1),
	})
	if err != nil {
		return "", false, fmt.Errorf("promptstore: query latest: %w", err)
	}
	if len(out.Items) == 0 {
		return "", false, nil
	}
	return strAttr(out.Items[0], "content"), true, nil
}

// Save appends a new version and returns its version id. Retries the
// same-millisecond sk collision up to maxSaveRetries.
func (s *Store) Save(ctx context.Context, workflowID, nodeID, field, content, savedBy string) (string, error) {
	for attempt := 0; attempt < maxSaveRetries; attempt++ {
		now := time.Now().UTC()
		sk := fmt.Sprintf("v#%016d", now.UnixMilli())
		_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(s.table),
			Item: map[string]types.AttributeValue{
				"pk":          &types.AttributeValueMemberS{Value: buildPK(workflowID, nodeID, field)},
				"sk":          &types.AttributeValueMemberS{Value: sk},
				"workflow_id": &types.AttributeValueMemberS{Value: workflowID},
				"node_id":     &types.AttributeValueMemberS{Value: nodeID},
				"field":       &types.AttributeValueMemberS{Value: field},
				"content":     &types.AttributeValueMemberS{Value: content},
				"saved_by":    &types.AttributeValueMemberS{Value: savedBy},
				"created_at":  &types.AttributeValueMemberS{Value: now.Format(time.RFC3339Nano)},
			},
			ConditionExpression: aws.String("attribute_not_exists(pk)"),
		})
		if err == nil {
			return sk, nil
		}
		var ccf *types.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			time.Sleep(2 * time.Millisecond)
			continue
		}
		return "", fmt.Errorf("promptstore: save: %w", err)
	}
	return "", fmt.Errorf("promptstore: save failed after %d retries", maxSaveRetries)
}

// History returns up to limit most-recent versions, newest first.
func (s *Store) History(ctx context.Context, workflowID, nodeID, field string, limit int) ([]Version, error) {
	if limit <= 0 {
		limit = 20
	}
	out, err := s.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(s.table),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: buildPK(workflowID, nodeID, field)},
		},
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int32(int32(limit)),
	})
	if err != nil {
		return nil, fmt.Errorf("promptstore: query history: %w", err)
	}
	versions := make([]Version, 0, len(out.Items))
	for _, item := range out.Items {
		created, _ := time.Parse(time.RFC3339Nano, strAttr(item, "created_at"))
		versions = append(versions, Version{
			Version:   strAttr(item, "sk"),
			Content:   strAttr(item, "content"),
			SavedBy:   strAttr(item, "saved_by"),
			CreatedAt: created,
		})
	}
	return versions, nil
}

func strAttr(item map[string]types.AttributeValue, name string) string {
	if v, ok := item[name].(*types.AttributeValueMemberS); ok {
		return v.Value
	}
	return ""
}
