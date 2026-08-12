// Package mongoquery — adapter.go wraps *mongo.Client from the Mongo driver
// v2 SDK so it satisfies MongoAPI. Kept in its own file to avoid bloating
// tool.go.
package mongoquery

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// clientAdapter bridges *mongo.Client to MongoAPI.
type clientAdapter struct{ c *mongo.Client }

// Compile-time assertion: clientAdapter must satisfy MongoAPI.
var _ MongoAPI = (*clientAdapter)(nil)

func (a *clientAdapter) Find(ctx context.Context, db, coll string, filter map[string]any, opts FindOpts) ([]map[string]any, error) {
	col := a.c.Database(db).Collection(coll)
	findOpts := options.Find()
	if len(opts.Projection) > 0 {
		findOpts.SetProjection(opts.Projection)
	}
	if len(opts.Sort) > 0 {
		findOpts.SetSort(opts.Sort)
	}
	if opts.Limit > 0 {
		findOpts.SetLimit(int64(opts.Limit))
	}
	cursor, err := col.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	out := []map[string]any{}
	for cursor.Next(ctx) {
		var m map[string]any
		if err := cursor.Decode(&m); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, cursor.Err()
}

func (a *clientAdapter) Count(ctx context.Context, db, coll string, filter map[string]any) (int64, error) {
	return a.c.Database(db).Collection(coll).CountDocuments(ctx, filter)
}

func (a *clientAdapter) Aggregate(ctx context.Context, db, coll string, pipeline []map[string]any) ([]map[string]any, error) {
	cursor, err := a.c.Database(db).Collection(coll).Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	out := []map[string]any{}
	for cursor.Next(ctx) {
		var m map[string]any
		if err := cursor.Decode(&m); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, cursor.Err()
}
