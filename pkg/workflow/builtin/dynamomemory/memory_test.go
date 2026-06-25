package dynamomemory

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

func msgItem(role string, text string) map[string]types.AttributeValue {
	blocks, _ := json.Marshal([]node.ContentBlock{{Type: node.BlockText, Text: text}})
	return map[string]types.AttributeValue{
		"role":    &types.AttributeValueMemberS{Value: role},
		"content": &types.AttributeValueMemberS{Value: string(blocks)},
	}
}

func TestLoad_emptySession_returnsNoMessages(t *testing.T) {
	f := &fakeDynamo{
		query: func(ctx context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			return &dynamodb.QueryOutput{Items: nil}, nil
		},
	}
	m := newTestMem(f)
	got, err := m.Load(context.Background(), node.MemoryKey{WorkflowID: "w", AccountID: "a", UserID: "u", SessionID: "s"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected 0 messages, got %d", len(got))
	}
}

func TestLoad_returnsMessagesInOrder(t *testing.T) {
	// Dynamo returns descending sk order: assistant (msg_seq=1) before user (msg_seq=0)
	items := []map[string]types.AttributeValue{
		msgItem("assistant", "hello"),
		msgItem("user", "hi"),
	}
	var capturedInput *dynamodb.QueryInput
	f := &fakeDynamo{
		query: func(ctx context.Context, in *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
			capturedInput = in
			return &dynamodb.QueryOutput{Items: items}, nil
		},
	}
	m := newTestMem(f)
	got, err := m.Load(context.Background(), node.MemoryKey{WorkflowID: "w", AccountID: "a", UserID: "u", SessionID: "s"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(got))
	}
	if got[0].Role != node.UserMsg || got[1].Role != node.AssistantMsg {
		t.Errorf("ordering wrong: %v %v", got[0].Role, got[1].Role)
	}
	if capturedInput == nil {
		t.Fatal("expected Query call")
	}
	if aws.ToBool(capturedInput.ScanIndexForward) != false {
		t.Errorf("ScanIndexForward should be false to fetch most-recent first")
	}
	wantLimit := int32(m.cfg.MaxTurns * 2)
	if aws.ToInt32(capturedInput.Limit) != wantLimit {
		t.Errorf("Limit = %d, want %d", aws.ToInt32(capturedInput.Limit), wantLimit)
	}
	if !strings.Contains(aws.ToString(capturedInput.KeyConditionExpression), "pk =") {
		t.Errorf("KeyConditionExpression missing pk =: %q", aws.ToString(capturedInput.KeyConditionExpression))
	}
	pkVal, ok := capturedInput.ExpressionAttributeValues[":pk"].(*types.AttributeValueMemberS)
	if !ok || pkVal.Value != "w#a#u#s" {
		t.Errorf("pk value = %v, want w#a#u#s", capturedInput.ExpressionAttributeValues[":pk"])
	}
}

func userAssistantTurn(userText, assistantText string) node.Turn {
	return node.Turn{
		User:      node.Message{Role: node.UserMsg, Content: []node.ContentBlock{{Type: node.BlockText, Text: userText}}},
		Assistant: node.Message{Role: node.AssistantMsg, Content: []node.ContentBlock{{Type: node.BlockText, Text: assistantText}}},
	}
}

func TestAppend_firstTurn_writesTwoItemsWithMatchingTimePrefix(t *testing.T) {
	var captured *dynamodb.TransactWriteItemsInput
	f := &fakeDynamo{
		transactWrite: func(ctx context.Context, in *dynamodb.TransactWriteItemsInput, _ ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			captured = in
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}
	m := newTestMem(f)
	err := m.Append(context.Background(), node.MemoryKey{WorkflowID: "w", AccountID: "a", UserID: "u", SessionID: "s"}, userAssistantTurn("hi", "hello"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if captured == nil || len(captured.TransactItems) != 2 {
		t.Fatalf("expected 2 transact items, got %v", captured)
	}
	skA := captured.TransactItems[0].Put.Item["sk"].(*types.AttributeValueMemberS).Value
	skB := captured.TransactItems[1].Put.Item["sk"].(*types.AttributeValueMemberS).Value
	if len(skA) < 15 || skA[13:] != "#0" {
		t.Errorf("sks[0] = %q; want 13-digit ms prefix then #0", skA)
	}
	if len(skB) < 15 || skB[13:] != "#1" {
		t.Errorf("sks[1] = %q; want 13-digit ms prefix then #1", skB)
	}
	if skA[:13] != skB[:13] {
		t.Errorf("user and assistant ms prefixes differ: %q vs %q", skA[:13], skB[:13])
	}
	roleA := captured.TransactItems[0].Put.Item["role"].(*types.AttributeValueMemberS).Value
	roleB := captured.TransactItems[1].Put.Item["role"].(*types.AttributeValueMemberS).Value
	if roleA != "user" || roleB != "assistant" {
		t.Errorf("roles = %q, %q; want user, assistant", roleA, roleB)
	}
	pk := captured.TransactItems[0].Put.Item["pk"].(*types.AttributeValueMemberS).Value
	if pk != "w#a#u#s" {
		t.Errorf("pk = %q, want w#a#u#s", pk)
	}
	if userAttr, ok := captured.TransactItems[0].Put.Item["user_id"].(*types.AttributeValueMemberS); !ok || userAttr.Value != "u" {
		t.Errorf("user_id attribute missing or wrong: %v", captured.TransactItems[0].Put.Item["user_id"])
	}
	if _, ok := captured.TransactItems[0].Put.Item["expires_at"]; !ok {
		t.Errorf("expires_at missing")
	}
	if _, ok := captured.TransactItems[0].Put.Item["session_id"]; !ok {
		t.Errorf("session_id missing")
	}
	if _, ok := captured.TransactItems[0].Put.Item["created_at_ms"]; !ok {
		t.Errorf("created_at_ms missing")
	}
}

func TestAppend_conditionalCheckFailed_retries(t *testing.T) {
	attempts := 0
	condFailure := &types.TransactionCanceledException{
		CancellationReasons: []types.CancellationReason{
			{Code: aws.String("ConditionalCheckFailed")},
			{Code: aws.String("None")},
		},
	}
	f := &fakeDynamo{
		transactWrite: func(ctx context.Context, in *dynamodb.TransactWriteItemsInput, _ ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
			attempts++
			if attempts < 3 {
				return nil, condFailure
			}
			return &dynamodb.TransactWriteItemsOutput{}, nil
		},
	}
	m := newTestMem(f)
	err := m.Append(context.Background(), node.MemoryKey{WorkflowID: "w", AccountID: "a", UserID: "u", SessionID: "s"}, userAssistantTurn("hi", "hello"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
	if f.queryCalls != 0 {
		t.Errorf("Append should make no Query calls under time-based SK; queryCalls = %d", f.queryCalls)
	}
}
