package keyviz

// Label is the canonical adapter-family label carried by KeyViz samples.
// The empty label is the legacy route-only view.
type Label string

const (
	LabelLegacy Label = ""
	LabelDynamo Label = "dynamo"
	LabelRedis  Label = "redis"
	LabelS3     Label = "s3"
	LabelSQS    Label = "sqs"
	LabelRawKV  Label = "rawkv"
)

// AllLabels lists every non-legacy adapter label that RegisterRoute can
// pre-create when label attribution is enabled. The fixed-size array makes
// accidental append-based mutation a compile-time error.
var AllLabels = [5]Label{LabelDynamo, LabelRedis, LabelS3, LabelSQS, LabelRawKV}

func allLabelsWithLegacy() []Label {
	result := make([]Label, len(AllLabels), len(AllLabels)+1)
	copy(result, AllLabels[:])
	return append(result, LabelLegacy)
}

type slotKey struct {
	RouteID uint64
	Label   Label
}
