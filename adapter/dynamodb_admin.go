package adapter

import (
	"context"
	"sort"
)

// AdminTableSummary is the table-level information the admin dashboard
// surfaces for a single Dynamo-compatible table. It deliberately
// projects only the fields the dashboard needs so the package's
// wire-format types (dynamoTableSchema and friends) stay internal.
type AdminTableSummary struct {
	Name                   string
	PartitionKey           string
	SortKey                string
	Generation             uint64
	GlobalSecondaryIndexes []AdminGSISummary
}

// AdminGSISummary mirrors AdminTableSummary for a single GSI.
type AdminGSISummary struct {
	Name           string
	PartitionKey   string
	SortKey        string
	ProjectionType string
}

// AdminListTables returns every Dynamo-style table this server knows
// about, in the lexicographic order the metadata index produces.
// Intended for the in-process admin listener as the SigV4-free
// counterpart to the listTables HTTP handler; both share the same
// underlying lookup so the two views cannot drift.
func (d *DynamoDBServer) AdminListTables(ctx context.Context) ([]string, error) {
	return d.listTableNames(ctx)
}

// AdminDescribeTable returns a schema snapshot for name. The triple
// (result, present, error) lets admin callers distinguish a genuine
// "not found" from a storage error without sniffing sentinels: when
// the table is missing the function returns (nil, false, nil).
func (d *DynamoDBServer) AdminDescribeTable(ctx context.Context, name string) (*AdminTableSummary, bool, error) {
	if err := d.ensureLegacyTableMigration(ctx, name); err != nil {
		return nil, false, err
	}
	schema, exists, err := d.loadTableSchema(ctx, name)
	if err != nil {
		return nil, false, err
	}
	if !exists {
		return nil, false, nil
	}
	return summaryFromSchema(schema), true, nil
}

func summaryFromSchema(s *dynamoTableSchema) *AdminTableSummary {
	out := &AdminTableSummary{
		Name:         s.TableName,
		PartitionKey: s.PrimaryKey.HashKey,
		SortKey:      s.PrimaryKey.RangeKey,
		Generation:   s.Generation,
	}
	if len(s.GlobalSecondaryIndexes) == 0 {
		return out
	}
	names := make([]string, 0, len(s.GlobalSecondaryIndexes))
	for n := range s.GlobalSecondaryIndexes {
		names = append(names, n)
	}
	// Sort so the JSON the admin handler emits is deterministic; map
	// iteration order would otherwise produce an unstable output that
	// breaks both UI diffing and snapshot tests.
	sort.Strings(names)
	out.GlobalSecondaryIndexes = make([]AdminGSISummary, 0, len(names))
	for _, name := range names {
		gsi := s.GlobalSecondaryIndexes[name]
		out.GlobalSecondaryIndexes = append(out.GlobalSecondaryIndexes, AdminGSISummary{
			Name:           name,
			PartitionKey:   gsi.KeySchema.HashKey,
			SortKey:        gsi.KeySchema.RangeKey,
			ProjectionType: gsi.Projection.ProjectionType,
		})
	}
	return out
}
