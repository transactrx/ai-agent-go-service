package promptstore

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// notFoundDynamo wraps fakeDynamo: DescribeTable fails once with
// ResourceNotFoundException, then succeeds ACTIVE.
type notFoundDynamo struct {
	*fakeDynamo
	calls int
}

func (n *notFoundDynamo) DescribeTable(ctx context.Context, in *dynamodb.DescribeTableInput, opt ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error) {
	n.calls++
	if n.calls == 1 {
		return nil, &types.ResourceNotFoundException{}
	}
	return n.fakeDynamo.DescribeTable(ctx, in, opt...)
}

func TestProvision_CreatesMissingTable(t *testing.T) {
	tablePollInterval = 0
	f := &notFoundDynamo{fakeDynamo: newFakeDynamo()}
	st := &Store{client: f, table: "opensearchaichatapi-assistant-prompts"}
	lg := log.New(os.Stdout, "", 0)
	if err := st.Provision(context.Background(), lg); err != nil {
		t.Fatalf("provision: %v", err)
	}
	if f.calls < 2 {
		t.Fatalf("expected describe→create→describe, calls=%d", f.calls)
	}
}

func TestProvision_TableExists(t *testing.T) {
	st := &Store{client: newFakeDynamo(), table: "t"}
	if err := st.Provision(context.Background(), log.New(os.Stdout, "", 0)); err != nil {
		t.Fatalf("provision: %v", err)
	}
}
