package adapter

import (
	"context"
	"encoding/base64"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	json "github.com/goccy/go-json"
)

// TestAdminPeekQueue_HappyPath sends three messages into a standard
// queue and confirms AdminPeekQueue surfaces all three with their
// bodies populated. Pins the basic wiring: meta load, vis-index scan,
// data-key follow-up read, body projection.
func TestAdminPeekQueue_HappyPath(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	queueURL := createSQSQueueForTest(t, node, "peek-basic")
	for i := range 3 {
		status, out := callSQS(t, node, sqsSendMessageTarget, map[string]any{
			"QueueUrl":    queueURL,
			"MessageBody": "body-" + strconv.Itoa(i),
		})
		if status != http.StatusOK {
			t.Fatalf("send #%d: %d %v", i, status, out)
		}
	}

	rows, nextCursor, err := node.sqsServer.AdminPeekQueue(context.Background(), fullAdminPrincipal, "peek-basic", AdminPeekMessageOptions{})
	if err != nil {
		t.Fatalf("AdminPeekQueue: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows=%d want 3; rows=%+v", len(rows), rows)
	}
	if nextCursor != "" {
		t.Fatalf("nextCursor=%q want empty (queue drained in one page)", nextCursor)
	}
	for i, row := range rows {
		assertPeekRowMatchesIndexedBody(t, i, row, "body-")
	}
}

// assertPeekRowMatchesIndexedBody pins the per-row invariants the
// happy path expects: the body matches bodyPrefix+i, no truncation
// fired (the bodies are short), MessageID is populated, and
// SentTimestamp is non-zero. Pulled into a helper so
// TestAdminPeekQueue_HappyPath stays under the cyclop budget.
func assertPeekRowMatchesIndexedBody(t *testing.T, idx int, row AdminPeekedMessage, bodyPrefix string) {
	t.Helper()
	want := bodyPrefix + strconv.Itoa(idx)
	if row.Body != want {
		t.Fatalf("rows[%d].Body=%q want %q", idx, row.Body, want)
	}
	if row.BodyTruncated {
		t.Fatalf("rows[%d].BodyTruncated=true; want false for %d-byte body", idx, len(row.Body))
	}
	if row.MessageID == "" {
		t.Fatalf("rows[%d].MessageID empty", idx)
	}
	if row.SentTimestamp.IsZero() {
		t.Fatalf("rows[%d].SentTimestamp zero", idx)
	}
}

// TestAdminPeekQueue_DoesNotChangeReceiveCount sends a message, peeks
// twice, then ReceiveMessage — the receive call must see
// ApproximateReceiveCount=1 (not 3). Pins the design's "peek is a
// pure read" invariant: the visibility timer is not started and no
// receipt-handle record is committed.
func TestAdminPeekQueue_DoesNotChangeReceiveCount(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	queueURL := createSQSQueueForTest(t, node, "peek-no-bump")
	status, _ := callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl":    queueURL,
		"MessageBody": "peek-me",
	})
	if status != http.StatusOK {
		t.Fatalf("send: %d", status)
	}

	for i := range 2 {
		rows, _, err := node.sqsServer.AdminPeekQueue(context.Background(), fullAdminPrincipal, "peek-no-bump", AdminPeekMessageOptions{})
		if err != nil {
			t.Fatalf("peek #%d: %v", i, err)
		}
		if len(rows) != 1 || rows[0].ReceiveCount != 0 {
			t.Fatalf("peek #%d: rows=%+v want 1 row with ReceiveCount=0", i, rows)
		}
	}

	status, out := callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": 1,
		"AttributeNames":      []string{"All"},
	})
	if status != http.StatusOK {
		t.Fatalf("receive: %d %v", status, out)
	}
	msgs, _ := out["Messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("receive: msgs=%d want 1", len(msgs))
	}
	m, _ := msgs[0].(map[string]any)
	attrs, _ := m["Attributes"].(map[string]any)
	if got, _ := attrs["ApproximateReceiveCount"].(string); got != "1" {
		t.Fatalf("ApproximateReceiveCount=%q want \"1\"; peek must not bump it (attrs=%v)", got, attrs)
	}
}

// TestAdminPeekQueue_BodyTruncation sends a long body and peeks with a
// small BodyMaxBytes. The returned Body is exactly BodyMaxBytes
// long; BodyTruncated is true; BodyOriginalSize records the full
// length.
func TestAdminPeekQueue_BodyTruncation(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	queueURL := createSQSQueueForTest(t, node, "peek-trunc")
	body := strings.Repeat("x", 4096)
	status, _ := callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl":    queueURL,
		"MessageBody": body,
	})
	if status != http.StatusOK {
		t.Fatalf("send: %d", status)
	}

	rows, _, err := node.sqsServer.AdminPeekQueue(context.Background(), fullAdminPrincipal, "peek-trunc", AdminPeekMessageOptions{BodyMaxBytes: 256})
	if err != nil {
		t.Fatalf("peek: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows=%d want 1", len(rows))
	}
	if len(rows[0].Body) != 256 {
		t.Fatalf("len(Body)=%d want 256", len(rows[0].Body))
	}
	if !rows[0].BodyTruncated {
		t.Fatalf("BodyTruncated=false want true")
	}
	if rows[0].BodyOriginalSize != int64(len(body)) {
		t.Fatalf("BodyOriginalSize=%d want %d", rows[0].BodyOriginalSize, len(body))
	}
}

// TestAdminPeekQueue_LimitClamping pins the [1, adminPeekMaxLimit]
// clamp + default-on-zero behaviour. With more rows than the cap in
// the queue, opts.Limit=500 must return adminPeekMaxLimit rows;
// opts.Limit=0 must return adminPeekDefaultLimit.
func TestAdminPeekQueue_LimitClamping(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	queueURL := createSQSQueueForTest(t, node, "peek-limit")
	const sent = 150
	for i := range sent {
		status, _ := callSQS(t, node, sqsSendMessageTarget, map[string]any{
			"QueueUrl":    queueURL,
			"MessageBody": "row-" + strconv.Itoa(i),
		})
		if status != http.StatusOK {
			t.Fatalf("send #%d: %d", i, status)
		}
	}

	rows, _, err := node.sqsServer.AdminPeekQueue(context.Background(), fullAdminPrincipal, "peek-limit", AdminPeekMessageOptions{})
	if err != nil {
		t.Fatalf("default-limit peek: %v", err)
	}
	if len(rows) != adminPeekDefaultLimit {
		t.Fatalf("default-limit peek: rows=%d want %d", len(rows), adminPeekDefaultLimit)
	}

	rows, _, err = node.sqsServer.AdminPeekQueue(context.Background(), fullAdminPrincipal, "peek-limit", AdminPeekMessageOptions{Limit: 500})
	if err != nil {
		t.Fatalf("over-limit peek: %v", err)
	}
	if len(rows) != adminPeekMaxLimit {
		t.Fatalf("over-limit peek: rows=%d want %d (clamped)", len(rows), adminPeekMaxLimit)
	}
}

// TestAdminPeekQueue_CursorRoundTrip drains a queue across two
// pages: peek returns N rows + nextCursor; peek-with-cursor returns
// the remaining rows; their union is the full queue with no
// overlaps and the final nextCursor is empty.
func TestAdminPeekQueue_CursorRoundTrip(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	queueURL := createSQSQueueForTest(t, node, "peek-cursor")
	const sent = 7
	for i := range sent {
		status, _ := callSQS(t, node, sqsSendMessageTarget, map[string]any{
			"QueueUrl":    queueURL,
			"MessageBody": "row-" + strconv.Itoa(i),
		})
		if status != http.StatusOK {
			t.Fatalf("send #%d: %d", i, status)
		}
	}

	ctx := context.Background()
	pageA, cursor := assertPeekPage(t, ctx, node, "peek-cursor", "", 4, 4, true)
	pageB, cursor := assertPeekPage(t, ctx, node, "peek-cursor", cursor, 10, sent-4, false)
	if cursor != "" {
		t.Fatalf("final cursor=%q want empty", cursor)
	}

	seen := make(map[string]int, sent)
	for _, row := range append(pageA, pageB...) {
		seen[row.MessageID]++
	}
	if len(seen) != sent {
		t.Fatalf("unique rows=%d want %d (overlap=%v)", len(seen), sent, seen)
	}
}

// assertPeekPage runs one peek call and pins (a) no error, (b) the
// expected row count, (c) whether a non-empty nextCursor is required.
// Pulled into a helper so TestAdminPeekQueue_CursorRoundTrip stays
// under the cyclop budget.
func assertPeekPage(t *testing.T, ctx context.Context, node Node, queue, cursorIn string, limit, wantRows int, wantCursor bool) ([]AdminPeekedMessage, string) {
	t.Helper()
	rows, cursorOut, err := node.sqsServer.AdminPeekQueue(ctx, fullAdminPrincipal, queue, AdminPeekMessageOptions{Limit: limit, Cursor: cursorIn})
	if err != nil {
		t.Fatalf("peek (cursor=%q): %v", cursorIn, err)
	}
	if len(rows) != wantRows {
		t.Fatalf("peek (cursor=%q) rows=%d want %d", cursorIn, len(rows), wantRows)
	}
	if wantCursor && cursorOut == "" {
		t.Fatalf("peek (cursor=%q) returned empty cursor; expected continuation", cursorIn)
	}
	return rows, cursorOut
}

// TestAdminPeekQueue_StaleGenerationCursor reproduces the failure
// mode the design doc §3.1 requires the cursor codec to catch:
// peek, then purge, then peek with the now-stale cursor → the
// adapter must reject with ErrAdminSQSValidation so the SPA refreshes
// from the front rather than returning rows from a purged generation.
func TestAdminPeekQueue_StaleGenerationCursor(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	queueURL := createSQSQueueForTest(t, node, "peek-stale")
	for i := range 5 {
		_, _ = callSQS(t, node, sqsSendMessageTarget, map[string]any{
			"QueueUrl":    queueURL,
			"MessageBody": "row-" + strconv.Itoa(i),
		})
	}

	ctx := context.Background()
	_, cursor, err := node.sqsServer.AdminPeekQueue(ctx, fullAdminPrincipal, "peek-stale", AdminPeekMessageOptions{Limit: 2})
	if err != nil {
		t.Fatalf("initial peek: %v", err)
	}
	if cursor == "" {
		t.Fatalf("cursor empty after partial page")
	}

	if _, err := node.sqsServer.AdminPurgeQueue(ctx, fullAdminPrincipal, "peek-stale"); err != nil {
		t.Fatalf("purge: %v", err)
	}

	_, _, err = node.sqsServer.AdminPeekQueue(ctx, fullAdminPrincipal, "peek-stale", AdminPeekMessageOptions{Cursor: cursor})
	if !errors.Is(err, ErrAdminSQSValidation) {
		t.Fatalf("stale-gen peek: want ErrAdminSQSValidation, got %v", err)
	}
}

// TestAdminPeekQueue_CursorMalformed pins each of the documented
// wire-shape failures (oversize, bad base64, bad JSON, unsupported
// version) onto ErrAdminSQSValidation rather than leaking the raw
// codec error.
func TestAdminPeekQueue_CursorMalformed(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	_ = createSQSQueueForTest(t, node, "peek-cursor-bad")

	cases := map[string]string{
		"oversize":      strings.Repeat("A", adminPeekCursorMaxBytes+1),
		"not-base64":    "!!!not_valid_base64!!!",
		"not-json":      base64.RawURLEncoding.EncodeToString([]byte("not-json")),
		"wrong-version": base64.RawURLEncoding.EncodeToString([]byte(`{"v":42,"gen":1}`)),
	}
	for name, cursor := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, err := node.sqsServer.AdminPeekQueue(context.Background(), fullAdminPrincipal, "peek-cursor-bad", AdminPeekMessageOptions{Cursor: cursor})
			if !errors.Is(err, ErrAdminSQSValidation) {
				t.Fatalf("%s: want ErrAdminSQSValidation, got %v", name, err)
			}
		})
	}
}

// TestAdminPeekQueue_ReadOnlyPrincipalAllowed confirms the canRead()
// gate accepts AdminRoleReadOnly. Peek is divergent from
// AdminPurgeQueue's canWrite() gate; this test pins the divergence so
// a future reviewer does not "fix" the inconsistency.
func TestAdminPeekQueue_ReadOnlyPrincipalAllowed(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	queueURL := createSQSQueueForTest(t, node, "peek-readonly")
	_, _ = callSQS(t, node, sqsSendMessageTarget, map[string]any{"QueueUrl": queueURL, "MessageBody": "x"})

	rows, _, err := node.sqsServer.AdminPeekQueue(context.Background(), readOnlyAdminPrincipal, "peek-readonly", AdminPeekMessageOptions{})
	if err != nil {
		t.Fatalf("read-only peek: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("read-only peek rows=%d want 1", len(rows))
	}
}

// TestAdminPeekQueue_RoleLessForbidden pins the gate's lower bound:
// the zero AdminPrincipal (no role set) is denied.
func TestAdminPeekQueue_RoleLessForbidden(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	_ = createSQSQueueForTest(t, node, "peek-noauth")

	_, _, err := node.sqsServer.AdminPeekQueue(context.Background(), AdminPrincipal{}, "peek-noauth", AdminPeekMessageOptions{})
	if !errors.Is(err, ErrAdminForbidden) {
		t.Fatalf("role-less peek: want ErrAdminForbidden, got %v", err)
	}
}

// TestAdminPeekQueue_MissingQueue maps to the structured 404 sentinel.
func TestAdminPeekQueue_MissingQueue(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	_, _, err := node.sqsServer.AdminPeekQueue(context.Background(), fullAdminPrincipal, "no-such-queue", AdminPeekMessageOptions{})
	if !errors.Is(err, ErrAdminSQSNotFound) {
		t.Fatalf("missing peek: want ErrAdminSQSNotFound, got %v", err)
	}
}

// TestAdminPeekQueue_EmptyName pins the up-front guard.
func TestAdminPeekQueue_EmptyName(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	for _, name := range []string{"", "   "} {
		_, _, err := node.sqsServer.AdminPeekQueue(context.Background(), fullAdminPrincipal, name, AdminPeekMessageOptions{})
		if !errors.Is(err, ErrAdminSQSValidation) {
			t.Fatalf("name=%q: want ErrAdminSQSValidation, got %v", name, err)
		}
	}
}

// TestAdminPeekQueue_DelayedMessageHidden confirms the visible_at <=
// now filter: a message with DelaySeconds > 0 does NOT appear in
// peek until its delay has elapsed. Matches receiveMessage's
// behaviour and pins the design's "currently-visible only" contract.
func TestAdminPeekQueue_DelayedMessageHidden(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	queueURL := createSQSQueueForTest(t, node, "peek-delayed")
	status, _ := callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl":     queueURL,
		"MessageBody":  "future",
		"DelaySeconds": 300,
	})
	if status != http.StatusOK {
		t.Fatalf("send: %d", status)
	}

	rows, _, err := node.sqsServer.AdminPeekQueue(context.Background(), fullAdminPrincipal, "peek-delayed", AdminPeekMessageOptions{})
	if err != nil {
		t.Fatalf("peek: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("rows=%d want 0 (message is delayed 5min)", len(rows))
	}
}

// TestAdminPeekQueue_FIFO_AttributesPopulated sends to a FIFO queue
// with GroupID + DeduplicationID and confirms the peek row carries
// both. Pins the field projection.
func TestAdminPeekQueue_FIFO_AttributesPopulated(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	const name = "peek-fifo.fifo"
	status, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName":  name,
		"Attributes": map[string]string{"FifoQueue": "true"},
	})
	if status != http.StatusOK {
		t.Fatalf("create fifo: %d %v", status, out)
	}
	queueURL, _ := out["QueueUrl"].(string)

	status, _ = callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl":               queueURL,
		"MessageBody":            "fifo-body",
		"MessageGroupId":         "tenant-7",
		"MessageDeduplicationId": "dedup-1",
	})
	if status != http.StatusOK {
		t.Fatalf("send fifo: %d", status)
	}

	rows, _, err := node.sqsServer.AdminPeekQueue(context.Background(), fullAdminPrincipal, name, AdminPeekMessageOptions{})
	if err != nil {
		t.Fatalf("peek fifo: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows=%d want 1", len(rows))
	}
	if rows[0].GroupID != "tenant-7" {
		t.Fatalf("GroupID=%q want \"tenant-7\"", rows[0].GroupID)
	}
	if rows[0].DeduplicationID != "dedup-1" {
		t.Fatalf("DeduplicationID=%q want \"dedup-1\"", rows[0].DeduplicationID)
	}
}

// TestAdminPeekQueue_AttributesProjected confirms typed
// MessageAttribute round-tripping: a String + a Binary attribute on
// the SendMessage call surface on the peek row with DataType /
// StringValue / BinaryValue preserved.
func TestAdminPeekQueue_AttributesProjected(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	queueURL := createSQSQueueForTest(t, node, "peek-attrs")
	binaryBytes := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	status, _ := callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl":    queueURL,
		"MessageBody": "x",
		"MessageAttributes": map[string]any{
			"Source": map[string]any{"DataType": "String", "StringValue": "checkout"},
			"Blob":   map[string]any{"DataType": "Binary", "BinaryValue": binaryBytes},
		},
	})
	if status != http.StatusOK {
		t.Fatalf("send: %d", status)
	}

	rows, _, err := node.sqsServer.AdminPeekQueue(context.Background(), fullAdminPrincipal, "peek-attrs", AdminPeekMessageOptions{})
	if err != nil {
		t.Fatalf("peek: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows=%d want 1", len(rows))
	}
	src, ok := rows[0].Attributes["Source"]
	if !ok || src.DataType != "String" || src.StringValue != "checkout" {
		t.Fatalf("Source attr=%+v want {String, checkout}", src)
	}
	blob, ok := rows[0].Attributes["Blob"]
	if !ok || blob.DataType != "Binary" || string(blob.BinaryValue) != string(binaryBytes) {
		t.Fatalf("Blob attr=%+v want {Binary, %x}", blob, binaryBytes)
	}
}

// TestAdminPeekQueue_PartitionedFIFO_Pagination installs a
// PartitionCount=4 FIFO queue, sends one message per group spread
// across all 4 partitions, then walks the peek cursor across pages
// (Limit=2 per page) until exhausted. Every sent message must
// surface exactly once across the union of pages, regardless of the
// rotated start partition. Pins the design's "rotated sequential
// scanning" contract.
func TestAdminPeekQueue_PartitionedFIFO_Pagination(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	const name = "peek-part.fifo"
	status, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName":  name,
		"Attributes": map[string]string{"FifoQueue": "true"},
	})
	if status != http.StatusOK {
		t.Fatalf("create: %d %v", status, out)
	}
	queueURL, _ := out["QueueUrl"].(string)
	installPartitionedMetaForTest(t, node, name, 4, htfifoThroughputPerMessageGroupID)

	groups := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"}
	sent := sendFIFOMessagesForPeek(t, node, queueURL, groups)

	ctx := context.Background()
	seen := walkPeekUntilDone(t, ctx, node, name, 2)
	assertEveryMessageSeenOnce(t, sent, seen)
}

// sendFIFOMessagesForPeek sends one message per group to a partitioned
// FIFO queue and returns the resulting set of message IDs. Pulled into
// a helper so TestAdminPeekQueue_PartitionedFIFO_Pagination stays
// under the cyclop budget.
func sendFIFOMessagesForPeek(t *testing.T, node Node, queueURL string, groups []string) map[string]struct{} {
	t.Helper()
	sent := make(map[string]struct{}, len(groups))
	for _, g := range groups {
		status, out := callSQS(t, node, sqsSendMessageTarget, map[string]any{
			"QueueUrl":               queueURL,
			"MessageBody":            "body-" + g,
			"MessageGroupId":         g,
			"MessageDeduplicationId": "dedup-" + g,
		})
		if status != http.StatusOK {
			t.Fatalf("send %s: %d %v", g, status, out)
		}
		msgID, _ := out["MessageId"].(string)
		if msgID == "" {
			t.Fatalf("send %s: empty MessageId", g)
		}
		sent[msgID] = struct{}{}
	}
	return sent
}

// walkPeekUntilDone pages through every peek result on a queue with
// the given per-page limit, returning a multi-set of how many times
// each MessageID was observed. Loop bound (32) is a safety net so a
// pagination bug terminates rather than runs forever; callers assert
// "saw every message exactly once" against this output.
func walkPeekUntilDone(t *testing.T, ctx context.Context, node Node, queue string, limit int) map[string]int {
	t.Helper()
	seen := make(map[string]int)
	cursor := ""
	for page := 0; page < 32; page++ {
		rows, next, err := node.sqsServer.AdminPeekQueue(ctx, fullAdminPrincipal, queue, AdminPeekMessageOptions{Limit: limit, Cursor: cursor})
		if err != nil {
			t.Fatalf("peek page %d: %v", page, err)
		}
		for _, row := range rows {
			seen[row.MessageID]++
		}
		if next == "" {
			return seen
		}
		cursor = next
	}
	t.Fatalf("peek did not terminate after 32 pages; seen=%v", seen)
	return seen
}

// assertEveryMessageSeenOnce pins the multi-set invariant the
// partitioned-FIFO pagination test cares about: every sent message
// surfaced in exactly one peek page, none missed and none duplicated.
func assertEveryMessageSeenOnce(t *testing.T, sent map[string]struct{}, seen map[string]int) {
	t.Helper()
	if len(seen) != len(sent) {
		want := make([]string, 0, len(sent))
		for id := range sent {
			want = append(want, id)
		}
		sort.Strings(want)
		got := make([]string, 0, len(seen))
		for id, count := range seen {
			got = append(got, id+"="+strconv.Itoa(count))
		}
		sort.Strings(got)
		t.Fatalf("seen=%v want %d unique messages (sent=%v)", got, len(sent), want)
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("message %q seen %d times (want exactly 1)", id, count)
		}
	}
}

// TestPeekCursorCodec_RoundTrip pins the encode/decode contract at
// the unit level so partition / start-partition / last-key all
// survive the base64url + JSON envelope.
func TestPeekCursorCodec_RoundTrip(t *testing.T) {
	t.Parallel()
	in := &peekCursor{
		V:              peekCursorSchemaV1,
		Generation:     42,
		StartPartition: 3,
		Partition:      5,
		LastKey:        []byte("some-last-key"),
	}
	encoded, err := encodePeekCursor(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if encoded == "" {
		t.Fatalf("encoded cursor empty")
	}
	out, err := decodePeekCursor(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out == nil {
		t.Fatalf("decode returned nil")
	}
	if out.V != in.V || out.Generation != in.Generation ||
		out.StartPartition != in.StartPartition || out.Partition != in.Partition ||
		string(out.LastKey) != string(in.LastKey) {
		t.Fatalf("round-trip mismatch: in=%+v out=%+v", in, out)
	}
}

// TestPeekCursorCodec_EmptyRoundTrip confirms the (nil, "") sentinel
// behaviour for "no cursor / start from front".
func TestPeekCursorCodec_EmptyRoundTrip(t *testing.T) {
	t.Parallel()
	encoded, err := encodePeekCursor(nil)
	if err != nil {
		t.Fatalf("encode(nil): %v", err)
	}
	if encoded != "" {
		t.Fatalf("encode(nil)=%q want empty", encoded)
	}
	out, err := decodePeekCursor("")
	if err != nil {
		t.Fatalf("decode(\"\"): %v", err)
	}
	if out != nil {
		t.Fatalf("decode(\"\")=%+v want nil", out)
	}
}

// TestPeekCursorCodec_LengthCap confirms the 512-byte hard cap by
// constructing a deliberately oversized cursor on the encode side
// (the design doc's "hard-capped at 512 bytes after encoding" rule).
// The encode path treats this as an internal regression rather than
// a client-side failure, so the test asserts an error rather than a
// truncated cursor.
func TestPeekCursorCodec_LengthCap(t *testing.T) {
	t.Parallel()
	bigKey := make([]byte, adminPeekCursorMaxBytes)
	for i := range bigKey {
		bigKey[i] = 'A'
	}
	_, err := encodePeekCursor(&peekCursor{V: peekCursorSchemaV1, LastKey: bigKey})
	if err == nil {
		t.Fatalf("encode of oversized cursor must error; got nil")
	}
}

// TestClampPeekLimit pins each branch of the clamp function.
func TestClampPeekLimit(t *testing.T) {
	t.Parallel()
	cases := map[int]int{
		0:                     adminPeekDefaultLimit,
		-5:                    adminPeekDefaultLimit,
		1:                     1,
		adminPeekDefaultLimit: adminPeekDefaultLimit,
		adminPeekMaxLimit:     adminPeekMaxLimit,
		adminPeekMaxLimit + 1: adminPeekMaxLimit,
		1_000_000:             adminPeekMaxLimit,
	}
	for in, want := range cases {
		if got := clampPeekLimit(in); got != want {
			t.Fatalf("clampPeekLimit(%d)=%d want %d", in, got, want)
		}
	}
}

// TestClampPeekBodyBytes pins each branch of the body-size clamp.
func TestClampPeekBodyBytes(t *testing.T) {
	t.Parallel()
	cases := map[int]int{
		0:                                       adminPeekDefaultBodyBytes,
		-5:                                      adminPeekDefaultBodyBytes,
		1:                                       adminPeekMinBodyBytes,
		adminPeekMinBodyBytes - 1:               adminPeekMinBodyBytes,
		adminPeekMinBodyBytes:                   adminPeekMinBodyBytes,
		adminPeekDefaultBodyBytes:               adminPeekDefaultBodyBytes,
		sqsMaximumAllowedMaximumMessageSize:     sqsMaximumAllowedMaximumMessageSize,
		sqsMaximumAllowedMaximumMessageSize + 1: sqsMaximumAllowedMaximumMessageSize,
		2 << 20:                                 sqsMaximumAllowedMaximumMessageSize,
	}
	for in, want := range cases {
		if got := clampPeekBodyBytes(in); got != want {
			t.Fatalf("clampPeekBodyBytes(%d)=%d want %d", in, got, want)
		}
	}
}

// TestPreparePeekCursor_FreshCursor confirms the first-call cursor
// stamps Generation and (for partitioned queues) StartPartition.
func TestPreparePeekCursor_FreshCursor(t *testing.T) {
	t.Parallel()

	t.Run("non-partitioned", func(t *testing.T) {
		t.Parallel()
		meta := &sqsQueueMeta{Generation: 7}
		out, err := preparePeekCursor(nil, meta, 0)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if out.V != peekCursorSchemaV1 || out.Generation != 7 || out.StartPartition != 0 || out.Partition != 0 {
			t.Fatalf("cursor=%+v want {V:1 Gen:7 SP:0 P:0}", out)
		}
	})

	t.Run("partitioned", func(t *testing.T) {
		t.Parallel()
		meta := &sqsQueueMeta{Generation: 11, PartitionCount: 4}
		out, err := preparePeekCursor(nil, meta, 2)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		if out.StartPartition != 2 || out.Partition != 2 {
			t.Fatalf("cursor=%+v want StartPartition=Partition=2", out)
		}
	})
}

// TestPreparePeekCursor_GenerationMismatch confirms a cursor from a
// prior generation is rejected as ErrAdminSQSValidation.
func TestPreparePeekCursor_GenerationMismatch(t *testing.T) {
	t.Parallel()
	stale := &peekCursor{V: peekCursorSchemaV1, Generation: 3}
	meta := &sqsQueueMeta{Generation: 4}
	_, err := preparePeekCursor(stale, meta, 0)
	if !errors.Is(err, ErrAdminSQSValidation) {
		t.Fatalf("want ErrAdminSQSValidation, got %v", err)
	}
}

// TestPreparePeekCursor_PartitionOutOfRange pins the Codex r1 P1
// fix: a cursor with StartPartition or Partition outside
// [0, max(PartitionCount, 1)) must be rejected with
// ErrAdminSQSValidation BEFORE the walk runs. Without the bounds
// check, walkPeek's `nextPart == cursor.StartPartition` termination
// condition never fires for an out-of-range StartPartition, looping
// ScanAt forever (request-amplification DoS against the admin
// endpoint).
func TestPreparePeekCursor_PartitionOutOfRange(t *testing.T) {
	t.Parallel()

	t.Run("partitioned: StartPartition >= PartitionCount", func(t *testing.T) {
		t.Parallel()
		meta := &sqsQueueMeta{Generation: 1, PartitionCount: 4}
		cursor := &peekCursor{V: peekCursorSchemaV1, Generation: 1, StartPartition: 99, Partition: 0}
		_, err := preparePeekCursor(cursor, meta, 0)
		if !errors.Is(err, ErrAdminSQSValidation) {
			t.Fatalf("StartPartition=99/PC=4: want ErrAdminSQSValidation, got %v", err)
		}
	})

	t.Run("partitioned: Partition >= PartitionCount", func(t *testing.T) {
		t.Parallel()
		meta := &sqsQueueMeta{Generation: 1, PartitionCount: 4}
		cursor := &peekCursor{V: peekCursorSchemaV1, Generation: 1, StartPartition: 0, Partition: 99}
		_, err := preparePeekCursor(cursor, meta, 0)
		if !errors.Is(err, ErrAdminSQSValidation) {
			t.Fatalf("Partition=99/PC=4: want ErrAdminSQSValidation, got %v", err)
		}
	})

	t.Run("non-partitioned: non-zero StartPartition", func(t *testing.T) {
		t.Parallel()
		meta := &sqsQueueMeta{Generation: 1, PartitionCount: 0}
		cursor := &peekCursor{V: peekCursorSchemaV1, Generation: 1, StartPartition: 1, Partition: 0}
		_, err := preparePeekCursor(cursor, meta, 0)
		if !errors.Is(err, ErrAdminSQSValidation) {
			t.Fatalf("non-partitioned StartPartition=1: want ErrAdminSQSValidation, got %v", err)
		}
	})

	t.Run("non-partitioned: non-zero Partition", func(t *testing.T) {
		t.Parallel()
		meta := &sqsQueueMeta{Generation: 1, PartitionCount: 1}
		cursor := &peekCursor{V: peekCursorSchemaV1, Generation: 1, StartPartition: 0, Partition: 5}
		_, err := preparePeekCursor(cursor, meta, 0)
		if !errors.Is(err, ErrAdminSQSValidation) {
			t.Fatalf("non-partitioned Partition=5: want ErrAdminSQSValidation, got %v", err)
		}
	})

	t.Run("partitioned: in-range cursor accepted", func(t *testing.T) {
		t.Parallel()
		meta := &sqsQueueMeta{Generation: 1, PartitionCount: 4}
		cursor := &peekCursor{V: peekCursorSchemaV1, Generation: 1, StartPartition: 3, Partition: 2}
		out, err := preparePeekCursor(cursor, meta, 0)
		if err != nil {
			t.Fatalf("in-range cursor: got err %v want nil", err)
		}
		if out.StartPartition != 3 || out.Partition != 2 {
			t.Fatalf("in-range cursor: got %+v want StartPartition=3 Partition=2", out)
		}
	})
}

// TestAdminPeekQueue_HostileCursorBoundedRequest is the end-to-end
// regression: an attacker crafts a wire-level cursor with an
// out-of-range partition and submits it. The call MUST return
// ErrAdminSQSValidation (not loop forever). The test runs with a
// short deadline so a regression terminates the test run rather
// than blocking CI.
func TestAdminPeekQueue_HostileCursorBoundedRequest(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	const name = "hostile.fifo"
	status, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName":  name,
		"Attributes": map[string]string{"FifoQueue": "true"},
	})
	if status != http.StatusOK {
		t.Fatalf("create: %d %v", status, out)
	}
	installPartitionedMetaForTest(t, node, name, 4, htfifoThroughputPerMessageGroupID)

	// Encode a cursor that names a partition far outside [0, 4).
	hostile, err := encodePeekCursor(&peekCursor{V: peekCursorSchemaV1, Generation: 1, StartPartition: 999, Partition: 999})
	if err != nil {
		t.Fatalf("encode hostile cursor: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err = node.sqsServer.AdminPeekQueue(ctx, fullAdminPrincipal, name, AdminPeekMessageOptions{Cursor: hostile})
	if !errors.Is(err, ErrAdminSQSValidation) {
		t.Fatalf("hostile cursor: want ErrAdminSQSValidation, got %v", err)
	}
}

// TestEncodePeekCursor_DecodesAsValidJSON spot-checks that the
// base64url envelope unwraps to recognisable JSON keys (so a SPA
// debugging session can inspect the cursor without the wire format
// being a black box).
func TestEncodePeekCursor_DecodesAsValidJSON(t *testing.T) {
	t.Parallel()
	encoded, err := encodePeekCursor(&peekCursor{V: peekCursorSchemaV1, Generation: 9, LastKey: []byte("k")})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	raw, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("base64 decode: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("json unmarshal: %v", err)
	}
	if _, ok := m["v"]; !ok {
		t.Fatalf("decoded JSON missing 'v': %v", m)
	}
	if _, ok := m["gen"]; !ok {
		t.Fatalf("decoded JSON missing 'gen': %v", m)
	}
}

// TestProjectPeekedAttributes_NilSafeAndTyped confirms the
// projection preserves DataType and binary payloads, and that an
// empty input yields nil (so JSON omitempty drops the field).
func TestProjectPeekedAttributes_NilSafeAndTyped(t *testing.T) {
	t.Parallel()

	if got := projectPeekedAttributes(nil); got != nil {
		t.Fatalf("nil input: got %v want nil", got)
	}
	if got := projectPeekedAttributes(map[string]sqsMessageAttributeValue{}); got != nil {
		t.Fatalf("empty input: got %v want nil", got)
	}

	in := map[string]sqsMessageAttributeValue{
		"A": {DataType: "String", StringValue: "foo"},
		"B": {DataType: "Binary", BinaryValue: []byte{1, 2, 3}},
	}
	out := projectPeekedAttributes(in)
	if out["A"].DataType != "String" || out["A"].StringValue != "foo" {
		t.Fatalf("A=%+v want {String, foo}", out["A"])
	}
	if out["B"].DataType != "Binary" || string(out["B"].BinaryValue) != string([]byte{1, 2, 3}) {
		t.Fatalf("B=%+v want {Binary, [1 2 3]}", out["B"])
	}
}

// TestAdminRole_CanRead pins the new canRead() gate's truth table.
func TestAdminRole_CanRead(t *testing.T) {
	t.Parallel()
	cases := map[AdminRole]bool{
		"":                false,
		AdminRoleReadOnly: true,
		AdminRoleFull:     true,
		"bogus":           false,
	}
	for role, want := range cases {
		if got := role.canRead(); got != want {
			t.Fatalf("AdminRole(%q).canRead()=%v want %v", role, got, want)
		}
	}
}

// silence "imported and not used" when only fixtures are referenced.
var _ = time.Now
