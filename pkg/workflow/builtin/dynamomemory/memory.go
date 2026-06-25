package dynamomemory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

const maxAppendRetries = 5

func buildPK(key node.MemoryKey) string {
	return key.WorkflowID + "#" + key.AccountID + "#" + key.UserID + "#" + key.SessionID
}

// Load returns up to MaxTurns*2 chronologically-most-recent messages for the
// key. Items are queried in descending sk order then reversed so the caller
// receives them oldest-to-newest within the most-recent window. The SK time
// prefix + msg_seq tiebreak guarantees user-before-assistant within a turn.
func (m *dynamoMemory) Load(ctx context.Context, key node.MemoryKey) ([]node.Message, error) {
	out, err := m.client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(m.cfg.TableName),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: buildPK(key)},
		},
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int32(int32(m.cfg.MaxTurns * 2)),
	})
	if err != nil {
		return nil, err
	}
	msgs := make([]node.Message, len(out.Items))
	for i, item := range out.Items {
		role := stringAttr(item, "role")
		contentStr := stringAttr(item, "content")
		var blocks []node.ContentBlock
		if err := json.Unmarshal([]byte(contentStr), &blocks); err != nil {
			return nil, fmt.Errorf("memory/dynamodb: decode content: %w", err)
		}
		// Place into the output slice in reverse so the caller sees chronological order.
		msgs[len(out.Items)-1-i] = node.Message{Role: node.MessageRole(role), Content: blocks}
	}
	return msgs, nil
}

func stringAttr(item map[string]types.AttributeValue, name string) string {
	if v, ok := item[name].(*types.AttributeValueMemberS); ok {
		return v.Value
	}
	return ""
}

// Append writes the user/assistant pair atomically with a time-based SK so no
// pre-Query is required to compute an ordering index. Retries on
// ConditionalCheckFailed up to maxAppendRetries (handles the unlikely
// same-millisecond collision between concurrent writers on the same session).
func (m *dynamoMemory) Append(ctx context.Context, key node.MemoryKey, t node.Turn) error {
	for attempt := 0; attempt < maxAppendRetries; attempt++ {
		err := m.appendOnce(ctx, key, t)
		if err == nil {
			return nil
		}
		var cancelled *types.TransactionCanceledException
		if errors.As(err, &cancelled) && isCondCheckFailed(cancelled) {
			continue
		}
		return err
	}
	return fmt.Errorf("memory/dynamodb: append failed after %d retries", maxAppendRetries)
}

func (m *dynamoMemory) appendOnce(ctx context.Context, key node.MemoryKey, t node.Turn) error {
	now := time.Now().UTC()
	expiresEpoch := now.Add(time.Duration(m.cfg.TTLDays) * 24 * time.Hour).Unix()
	skPrefix := fmt.Sprintf("%013d", now.UnixMilli())
	pk := buildPK(key)

	items := make([]types.TransactWriteItem, 0, 2)
	for _, p := range []struct {
		seq int
		msg node.Message
	}{{0, t.User}, {1, t.Assistant}} {
		contentBytes, _ := json.Marshal(p.msg.Content)
		sk := skPrefix + "#" + strconv.Itoa(p.seq)
		put := &types.Put{
			TableName: aws.String(m.cfg.TableName),
			Item: map[string]types.AttributeValue{
				"pk":            &types.AttributeValueMemberS{Value: pk},
				"sk":            &types.AttributeValueMemberS{Value: sk},
				"id":            &types.AttributeValueMemberS{Value: uuid.NewString()},
				"workflow_id":   &types.AttributeValueMemberS{Value: key.WorkflowID},
				"account_id":    &types.AttributeValueMemberS{Value: key.AccountID},
				"user_id":       &types.AttributeValueMemberS{Value: key.UserID},
				"session_id":    &types.AttributeValueMemberS{Value: key.SessionID},
				"role":          &types.AttributeValueMemberS{Value: string(p.msg.Role)},
				"msg_seq":       &types.AttributeValueMemberN{Value: strconv.Itoa(p.seq)},
				"content":       &types.AttributeValueMemberS{Value: string(contentBytes)},
				"created_at":    &types.AttributeValueMemberS{Value: now.Format(time.RFC3339Nano)},
				"created_at_ms": &types.AttributeValueMemberN{Value: strconv.FormatInt(now.UnixMilli(), 10)},
				"expires_at":    &types.AttributeValueMemberN{Value: strconv.FormatInt(expiresEpoch, 10)},
			},
			ConditionExpression: aws.String("attribute_not_exists(pk)"),
		}
		items = append(items, types.TransactWriteItem{Put: put})
	}
	_, err := m.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: items})
	return err
}

func isCondCheckFailed(c *types.TransactionCanceledException) bool {
	for _, r := range c.CancellationReasons {
		if aws.ToString(r.Code) == "ConditionalCheckFailed" {
			return true
		}
	}
	return false
}
