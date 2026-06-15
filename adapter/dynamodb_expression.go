package adapter

import (
	"bytes"
	"maps"
	"math/big"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

func replaceNames(expr string, names map[string]string) (string, error) {
	if expr == "" || len(names) == 0 {
		return expr, nil
	}
	if err := validateExpressionAttributeNames(names); err != nil {
		return "", err
	}
	keys := make([]string, 0, len(names))
	for k := range names {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if len(keys[i]) == len(keys[j]) {
			return keys[i] < keys[j]
		}
		return len(keys[i]) > len(keys[j])
	})

	// DynamoDB expression attribute names are substituted once.
	args := make([]string, 0, len(keys)*replacerArgPairSize)
	for _, key := range keys {
		args = append(args, key, names[key])
	}
	return strings.NewReplacer(args...).Replace(expr), nil
}

func validateExpressionAttributeNames(names map[string]string) error {
	for placeholder, name := range names {
		if !isExpressionAttributePlaceholder(placeholder) {
			return errors.Errorf("invalid expression attribute placeholder %q", placeholder)
		}
		if !isExpressionAttributeName(name) {
			return errors.Errorf("invalid expression attribute name %q for placeholder %q", name, placeholder)
		}
	}
	return nil
}

func isExpressionAttributePlaceholder(s string) bool {
	if len(s) <= 1 || s[0] != '#' {
		return false
	}
	return isExpressionPlaceholderIdentifier(s[1:])
}

func isExpressionPlaceholderIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if isExpressionPlaceholderIdentByte(s[i]) {
			continue
		}
		return false
	}
	return true
}

func isExpressionPlaceholderIdentByte(b byte) bool {
	return b == '_' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}

func isExpressionAttributeName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if isExpressionAttributeNameByte(s[i]) {
			continue
		}
		return false
	}
	return true
}

func isExpressionAttributeNameByte(b byte) bool {
	return b == '_' || b == '.' || b == '-' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}

func applyUpdateExpression(expr string, names map[string]string, values map[string]attributeValue, item map[string]attributeValue) error {
	updExpr, err := replaceNames(expr, names)
	if err != nil {
		return err
	}
	updExpr = strings.TrimSpace(updExpr)
	sections, err := parseUpdateExpressionSections(updExpr)
	if err != nil {
		return err
	}
	for _, section := range sections {
		if err := applyUpdateExpressionSection(section, values, item); err != nil {
			return err
		}
	}
	return nil
}

type updateExpressionSection struct {
	action string
	body   string
}

func parseUpdateExpressionSections(expr string) ([]updateExpressionSection, error) {
	if strings.TrimSpace(expr) == "" {
		return nil, errors.New("unsupported update expression")
	}
	sections := make([]updateExpressionSection, 0, updateSplitCount)
	seen := map[string]struct{}{}
	i := skipSpaces(expr, 0)
	for i < len(expr) {
		action, nextPos, ok := parseUpdateActionToken(expr, i)
		if !ok {
			return nil, errors.New("unsupported update expression")
		}
		if _, exists := seen[action]; exists {
			return nil, errors.New("duplicate update action")
		}
		seen[action] = struct{}{}
		bodyStart := skipSpaces(expr, nextPos)
		bodyEnd := findNextUpdateAction(expr, bodyStart)
		if bodyEnd < 0 {
			bodyEnd = len(expr)
		}
		body := strings.TrimSpace(expr[bodyStart:bodyEnd])
		if body == "" {
			return nil, errors.New("unsupported update expression")
		}
		sections = append(sections, updateExpressionSection{action: action, body: body})
		if bodyEnd >= len(expr) {
			break
		}
		i = bodyEnd
	}
	if len(sections) == 0 {
		return nil, errors.New("unsupported update expression")
	}
	return sections, nil
}

func applyUpdateExpressionSection(section updateExpressionSection, values map[string]attributeValue, item map[string]attributeValue) error {
	switch section.action {
	case "SET":
		return applySetUpdateAction(section.body, values, item)
	case "REMOVE":
		return applyRemoveUpdateAction(section.body, item)
	case "ADD":
		return applyAddUpdateAction(section.body, values, item)
	case "DELETE":
		return applyDeleteUpdateAction(section.body, values, item)
	default:
		return errors.New("unsupported update action")
	}
}

func parseUpdateActionToken(expr string, pos int) (string, int, bool) {
	actions := []string{"SET", "REMOVE", "ADD", "DELETE"}
	for _, action := range actions {
		end := pos + len(action)
		if end > len(expr) {
			continue
		}
		if !strings.EqualFold(expr[pos:end], action) {
			continue
		}
		if !isLogicalKeywordBoundary(expr, pos-1) || !isLogicalKeywordBoundary(expr, end) {
			continue
		}
		return action, end, true
	}
	return "", 0, false
}

func skipSpaces(expr string, pos int) int {
	for pos < len(expr) && (expr[pos] == ' ' || expr[pos] == '\t' || expr[pos] == '\n' || expr[pos] == '\r') {
		pos++
	}
	return pos
}

func findNextUpdateAction(expr string, start int) int {
	depth := 0
	for i := start; i < len(expr); i++ {
		depth = nextParenDepth(depth, expr[i])
		if depth != 0 {
			continue
		}
		_, _, ok := parseUpdateActionToken(expr, i)
		if ok {
			return i
		}
	}
	return -1
}

func applySetUpdateAction(body string, values map[string]attributeValue, item map[string]attributeValue) error {
	assignments, err := splitTopLevelByComma(body)
	if err != nil {
		return errors.New("invalid update expression")
	}
	for _, assignment := range assignments {
		if err := applySingleSetAssignment(assignment, values, item); err != nil {
			return err
		}
	}
	return nil
}

func applySingleSetAssignment(assignment string, values map[string]attributeValue, item map[string]attributeValue) error {
	parts := strings.SplitN(assignment, "=", updateSplitCount)
	if len(parts) != updateSplitCount {
		return errors.New("invalid update expression")
	}
	path := strings.TrimSpace(parts[0])
	if path == "" {
		return errors.New("invalid update expression attribute")
	}
	valueExpr := strings.TrimSpace(parts[1])
	valueAttr, err := evalUpdateValueExpression(valueExpr, values, item)
	if err != nil {
		return err
	}
	return setDocumentPath(item, path, valueAttr)
}

func applyRemoveUpdateAction(body string, item map[string]attributeValue) error {
	attrs, err := splitTopLevelByComma(body)
	if err != nil {
		return errors.New("invalid update expression")
	}
	for _, attr := range attrs {
		path := strings.TrimSpace(attr)
		if path == "" {
			return errors.New("invalid update expression attribute")
		}
		if err := removeDocumentPath(item, path); err != nil {
			return err
		}
	}
	return nil
}

func applyAddUpdateAction(body string, values map[string]attributeValue, item map[string]attributeValue) error {
	terms, err := splitTopLevelByComma(body)
	if err != nil {
		return errors.New("invalid update expression")
	}
	for _, term := range terms {
		if err := applySingleAddTerm(term, values, item); err != nil {
			return err
		}
	}
	return nil
}

func applySingleAddTerm(term string, values map[string]attributeValue, item map[string]attributeValue) error {
	parts := strings.Fields(term)
	if len(parts) != updateSplitCount {
		return errors.New("invalid update expression")
	}
	path := strings.TrimSpace(parts[0])
	placeholder := strings.TrimSpace(parts[1])
	if path == "" || !strings.HasPrefix(placeholder, ":") {
		return errors.New("invalid update expression")
	}
	addValue, ok := values[placeholder]
	if !ok {
		return errors.New("missing value attribute")
	}
	current, exists, err := resolveDocumentPath(item, path)
	if err != nil {
		return err
	}
	next, err := addAttributeValue(current, exists, addValue)
	if err != nil {
		return err
	}
	return setDocumentPath(item, path, next)
}

func addNumericAttributeValues(left string, right string) (string, error) {
	leftRat, rightRat := &big.Rat{}, &big.Rat{}
	if _, ok := leftRat.SetString(strings.TrimSpace(left)); !ok {
		return "", errors.New("invalid number attribute")
	}
	if _, ok := rightRat.SetString(strings.TrimSpace(right)); !ok {
		return "", errors.New("invalid number attribute")
	}
	sum := &big.Rat{}
	sum.Add(leftRat, rightRat)
	if sum.IsInt() {
		return sum.Num().String(), nil
	}
	out := strings.TrimRight(sum.FloatString(numericUpdateScaleDigits), "0")
	out = strings.TrimRight(out, ".")
	if out == "" {
		return "0", nil
	}
	return out, nil
}

func applyDeleteUpdateAction(body string, values map[string]attributeValue, item map[string]attributeValue) error {
	terms, err := splitTopLevelByComma(body)
	if err != nil {
		return errors.New("invalid update expression")
	}
	for _, term := range terms {
		if err := applySingleDeleteTerm(term, values, item); err != nil {
			return err
		}
	}
	return nil
}

func applySingleDeleteTerm(term string, values map[string]attributeValue, item map[string]attributeValue) error {
	fields := strings.Fields(strings.TrimSpace(term))
	switch len(fields) {
	case 0:
		return errors.New("invalid update expression")
	case 1:
		return removeDocumentPath(item, fields[0])
	case updateSplitCount:
		return applyDeleteSetTerm(fields[0], fields[1], values, item)
	default:
		return errors.New("invalid update expression")
	}
}

func applyDeleteSetTerm(pathExpr string, placeholderExpr string, values map[string]attributeValue, item map[string]attributeValue) error {
	path := strings.TrimSpace(pathExpr)
	placeholder := strings.TrimSpace(placeholderExpr)
	if path == "" || !strings.HasPrefix(placeholder, ":") {
		return errors.New("invalid update expression")
	}
	deleteValue, ok := values[placeholder]
	if !ok {
		return errors.New("missing value attribute")
	}
	current, found, err := resolveDocumentPath(item, path)
	if err != nil || !found {
		return err
	}
	next, removeAttr, err := deleteAttributeValueElements(current, deleteValue)
	if err != nil {
		return err
	}
	if removeAttr {
		return removeDocumentPath(item, path)
	}
	return setDocumentPath(item, path, next)
}

func splitTopLevelByComma(expr string) ([]string, error) {
	depth := 0
	last := 0
	parts := make([]string, 0, splitPartsInitialCapacity)
	for i := 0; i < len(expr); i++ {
		depth = nextParenDepth(depth, expr[i])
		if depth < 0 {
			return nil, errors.New("invalid expression")
		}
		if depth != 0 || expr[i] != ',' {
			continue
		}
		part := strings.TrimSpace(expr[last:i])
		if part == "" {
			return nil, errors.New("invalid expression")
		}
		parts = append(parts, part)
		last = i + 1
	}
	if depth != 0 {
		return nil, errors.New("invalid expression")
	}
	tail := strings.TrimSpace(expr[last:])
	if tail == "" {
		return nil, errors.New("invalid expression")
	}
	return append(parts, tail), nil
}

type documentPathToken struct {
	attr    string
	index   int
	isIndex bool
}

func parseDocumentPath(path string) ([]documentPathToken, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("invalid document path")
	}
	tokens := make([]documentPathToken, 0, updateSplitCount)
	for pos := 0; pos < len(path); {
		nextPos, token, err := consumeDocumentPathToken(path, pos)
		if err != nil {
			return nil, err
		}
		pos = nextPos
		if token.attr != "" || token.isIndex {
			tokens = append(tokens, token)
		}
	}
	if len(tokens) == 0 {
		return nil, errors.New("invalid document path")
	}
	return tokens, nil
}

func consumeDocumentPathToken(path string, pos int) (int, documentPathToken, error) {
	switch path[pos] {
	case '.':
		return pos + 1, documentPathToken{}, nil
	case '[':
		return consumeDocumentPathIndex(path, pos)
	default:
		return consumeDocumentPathAttr(path, pos)
	}
}

func consumeDocumentPathIndex(path string, pos int) (int, documentPathToken, error) {
	end := strings.IndexByte(path[pos:], ']')
	if end <= 1 {
		return 0, documentPathToken{}, errors.New("invalid document path")
	}
	indexValue, err := strconv.Atoi(path[pos+1 : pos+end])
	if err != nil || indexValue < 0 {
		return 0, documentPathToken{}, errors.New("invalid document path")
	}
	return pos + end + 1, documentPathToken{index: indexValue, isIndex: true}, nil
}

func consumeDocumentPathAttr(path string, pos int) (int, documentPathToken, error) {
	start := pos
	for pos < len(path) && path[pos] != '.' && path[pos] != '[' {
		pos++
	}
	attr := strings.TrimSpace(path[start:pos])
	if attr == "" {
		return 0, documentPathToken{}, errors.New("invalid document path")
	}
	return pos, documentPathToken{attr: attr}, nil
}

func resolveDocumentPath(item map[string]attributeValue, path string) (attributeValue, bool, error) {
	tokens, err := parseDocumentPath(path)
	if err != nil {
		return attributeValue{}, false, err
	}
	current := attributeValue{M: item}
	found := true
	for _, token := range tokens {
		current, found = nextDocumentPathValue(current, found, token)
		if !found {
			return attributeValue{}, false, nil
		}
	}
	return cloneAttributeValue(current), true, nil
}

func nextDocumentPathValue(current attributeValue, found bool, token documentPathToken) (attributeValue, bool) {
	if !found {
		return attributeValue{}, false
	}
	if token.isIndex {
		if !current.hasListType() || token.index >= len(current.L) {
			return attributeValue{}, false
		}
		return current.L[token.index], true
	}
	if !current.hasMapType() {
		return attributeValue{}, false
	}
	value, ok := current.M[token.attr]
	if !ok {
		return attributeValue{}, false
	}
	return value, true
}

func setDocumentPath(item map[string]attributeValue, path string, value attributeValue) error {
	tokens, err := parseDocumentPath(path)
	if err != nil {
		return err
	}
	root, err := setDocumentPathValue(attributeValue{M: cloneAttributeValueMap(item)}, true, tokens, value)
	if err != nil {
		return err
	}
	replaceAttributeValueMap(item, root.M)
	return nil
}

func setDocumentPathValue(current attributeValue, exists bool, tokens []documentPathToken, value attributeValue) (attributeValue, error) {
	if len(tokens) == 0 {
		return cloneAttributeValue(value), nil
	}
	token := tokens[0]
	if token.isIndex {
		return setDocumentPathIndex(current, exists, token, tokens[1:], value)
	}
	return setDocumentPathAttribute(current, exists, token, tokens[1:], value)
}

func setDocumentPathIndex(current attributeValue, exists bool, token documentPathToken, rest []documentPathToken, value attributeValue) (attributeValue, error) {
	if !exists || !current.hasListType() {
		return attributeValue{}, errors.New("invalid document path")
	}
	list := cloneAttributeValueList(current.L)
	if token.index > len(list) {
		return attributeValue{}, errors.New("invalid document path")
	}
	if token.index == len(list) {
		return appendDocumentPathIndex(list, rest, value)
	}
	nextValue, err := setDocumentPathValue(list[token.index], true, rest, value)
	if err != nil {
		return attributeValue{}, err
	}
	list[token.index] = nextValue
	return attributeValue{L: list}, nil
}

func appendDocumentPathIndex(list []attributeValue, rest []documentPathToken, value attributeValue) (attributeValue, error) {
	child := value
	if len(rest) > 0 {
		var err error
		child, err = setDocumentPathValue(newDocumentContainer(rest[0]), true, rest, value)
		if err != nil {
			return attributeValue{}, err
		}
	}
	list = append(list, cloneAttributeValue(child))
	return attributeValue{L: list}, nil
}

func setDocumentPathAttribute(current attributeValue, exists bool, token documentPathToken, rest []documentPathToken, value attributeValue) (attributeValue, error) {
	var object map[string]attributeValue
	if exists {
		if !current.hasMapType() {
			return attributeValue{}, errors.New("invalid document path")
		}
		object = cloneAttributeValueMap(current.M)
	} else {
		object = map[string]attributeValue{}
	}
	child, childExists := object[token.attr]
	if !childExists && len(rest) > 0 {
		child = newDocumentContainer(rest[0])
		childExists = true
	}
	nextValue, err := setDocumentPathValue(child, childExists, rest, value)
	if err != nil {
		return attributeValue{}, err
	}
	object[token.attr] = nextValue
	return attributeValue{M: object}, nil
}

func newDocumentContainer(next documentPathToken) attributeValue {
	if next.isIndex {
		return attributeValue{L: []attributeValue{}}
	}
	return attributeValue{M: map[string]attributeValue{}}
}

func removeDocumentPath(item map[string]attributeValue, path string) error {
	tokens, err := parseDocumentPath(path)
	if err != nil {
		return err
	}
	root, err := removeDocumentPathValue(attributeValue{M: cloneAttributeValueMap(item)}, true, tokens)
	if err != nil {
		return err
	}
	replaceAttributeValueMap(item, root.M)
	return nil
}

func removeDocumentPathValue(current attributeValue, exists bool, tokens []documentPathToken) (attributeValue, error) {
	if !exists || len(tokens) == 0 {
		return current, nil
	}
	token := tokens[0]
	if token.isIndex {
		return removeDocumentPathIndex(current, token, tokens[1:])
	}
	return removeDocumentPathAttribute(current, token, tokens[1:])
}

func removeDocumentPathIndex(current attributeValue, token documentPathToken, rest []documentPathToken) (attributeValue, error) {
	if !current.hasListType() || token.index >= len(current.L) {
		return current, nil
	}
	list := cloneAttributeValueList(current.L)
	if len(rest) == 0 {
		list = append(list[:token.index], list[token.index+1:]...)
		return attributeValue{L: list}, nil
	}
	nextValue, err := removeDocumentPathValue(list[token.index], true, rest)
	if err != nil {
		return attributeValue{}, err
	}
	list[token.index] = nextValue
	return attributeValue{L: list}, nil
}

func removeDocumentPathAttribute(current attributeValue, token documentPathToken, rest []documentPathToken) (attributeValue, error) {
	if !current.hasMapType() {
		return current, nil
	}
	object := cloneAttributeValueMap(current.M)
	child, ok := object[token.attr]
	if !ok {
		return current, nil
	}
	if len(rest) == 0 {
		delete(object, token.attr)
		return attributeValue{M: object}, nil
	}
	nextValue, err := removeDocumentPathValue(child, true, rest)
	if err != nil {
		return attributeValue{}, err
	}
	object[token.attr] = nextValue
	return attributeValue{M: object}, nil
}

func replaceAttributeValueMap(dst map[string]attributeValue, src map[string]attributeValue) {
	clear(dst)
	maps.Copy(dst, src)
}

func deleteAttributeValueElements(current attributeValue, deleteValue attributeValue) (attributeValue, bool, error) {
	currentKind, _ := detectAttributeValueKind(current)
	deleteKind, _ := detectAttributeValueKind(deleteValue)
	if currentKind != deleteKind {
		return attributeValue{}, false, errors.New("DELETE supports only matching set attribute types")
	}
	switch currentKind {
	case attributeValueKindStringSet:
		return buildDeleteSetResult(attributeValue{SS: subtractStringSet(current.SS, deleteValue.SS)})
	case attributeValueKindNumberSet:
		return buildDeleteSetResult(attributeValue{NS: subtractNumberSet(current.NS, deleteValue.NS)})
	case attributeValueKindBinarySet:
		return buildDeleteSetResult(attributeValue{BS: subtractBinarySet(current.BS, deleteValue.BS)})
	case attributeValueKindInvalid,
		attributeValueKindString,
		attributeValueKindNumber,
		attributeValueKindBinary,
		attributeValueKindBool,
		attributeValueKindNull,
		attributeValueKindList,
		attributeValueKindMap:
		return attributeValue{}, false, errors.New("DELETE supports only matching set attribute types")
	}
	return attributeValue{}, false, errors.New("DELETE supports only matching set attribute types")
}

func buildDeleteSetResult(next attributeValue) (attributeValue, bool, error) {
	if next.hasStringSetType() && len(next.SS) == 0 {
		return attributeValue{}, true, nil
	}
	if next.hasNumberSetType() && len(next.NS) == 0 {
		return attributeValue{}, true, nil
	}
	if next.hasBinarySetType() && len(next.BS) == 0 {
		return attributeValue{}, true, nil
	}
	return next, false, nil
}

func subtractStringSet(current []string, remove []string) []string {
	removeSet := make(map[string]struct{}, len(remove))
	for _, value := range remove {
		removeSet[value] = struct{}{}
	}
	out := make([]string, 0, len(current))
	for _, value := range current {
		if _, ok := removeSet[value]; ok {
			continue
		}
		out = append(out, value)
	}
	return out
}

func subtractNumberSet(current []string, remove []string) []string {
	removeSet := make(map[string]struct{}, len(remove))
	for _, value := range remove {
		removeSet[canonicalNumberString(value)] = struct{}{}
	}
	out := make([]string, 0, len(current))
	for _, value := range current {
		if _, ok := removeSet[canonicalNumberString(value)]; ok {
			continue
		}
		out = append(out, value)
	}
	return out
}

func subtractBinarySet(current [][]byte, remove [][]byte) [][]byte {
	removeSet := make(map[string]struct{}, len(remove))
	for _, value := range remove {
		removeSet[string(value)] = struct{}{}
	}
	out := make([][]byte, 0, len(current))
	for _, value := range current {
		if _, ok := removeSet[string(value)]; ok {
			continue
		}
		out = append(out, bytes.Clone(value))
	}
	return out
}

func evalUpdateValueExpression(expr string, values map[string]attributeValue, item map[string]attributeValue) (attributeValue, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return attributeValue{}, errors.New("invalid update expression")
	}
	if value, handled, err := evalArithmeticUpdateOperand(expr, values, item); handled {
		return value, err
	}
	if value, handled, err := evalNamedUpdateFunction(expr, values, item, "if_not_exists", evalIfNotExistsUpdateValue); handled {
		return value, err
	}
	if value, handled, err := evalNamedUpdateFunction(expr, values, item, "list_append", evalListAppendUpdateValue); handled {
		return value, err
	}
	return evalUpdateTerminalValue(expr, values, item)
}

func evalArithmeticUpdateOperand(expr string, values map[string]attributeValue, item map[string]attributeValue) (attributeValue, bool, error) {
	index, op, ok := findTopLevelArithmeticOperator(expr)
	if !ok {
		return attributeValue{}, false, nil
	}
	left, err := evalUpdateValueExpression(expr[:index], values, item)
	if err != nil {
		return attributeValue{}, true, err
	}
	right, err := evalUpdateValueExpression(expr[index+1:], values, item)
	if err != nil {
		return attributeValue{}, true, err
	}
	value, err := applyArithmeticUpdateValue(left, right, op)
	return value, true, err
}

func evalNamedUpdateFunction(
	expr string,
	values map[string]attributeValue,
	item map[string]attributeValue,
	name string,
	eval func([]string, map[string]attributeValue, map[string]attributeValue) (attributeValue, error),
) (attributeValue, bool, error) {
	args, ok, err := parseExpressionFunctionArgs(expr, name)
	if err != nil {
		return attributeValue{}, true, err
	}
	if !ok {
		return attributeValue{}, false, nil
	}
	value, err := eval(args, values, item)
	return value, true, err
}

func evalUpdateTerminalValue(expr string, values map[string]attributeValue, item map[string]attributeValue) (attributeValue, error) {
	if strings.HasPrefix(expr, ":") {
		value, ok := values[expr]
		if !ok {
			return attributeValue{}, errors.New("missing value attribute")
		}
		return cloneAttributeValue(value), nil
	}
	value, found, err := resolveDocumentPath(item, expr)
	if err != nil {
		return attributeValue{}, err
	}
	if !found {
		return attributeValue{}, errors.New("missing value attribute")
	}
	return value, nil
}

func evalIfNotExistsUpdateValue(args []string, values map[string]attributeValue, item map[string]attributeValue) (attributeValue, error) {
	if len(args) != updateSplitCount {
		return attributeValue{}, errors.New("invalid update expression")
	}
	current, found, err := resolveDocumentPath(item, strings.TrimSpace(args[0]))
	if err != nil {
		return attributeValue{}, err
	}
	if found {
		return current, nil
	}
	return evalUpdateValueExpression(args[1], values, item)
}

func evalListAppendUpdateValue(args []string, values map[string]attributeValue, item map[string]attributeValue) (attributeValue, error) {
	if len(args) != updateSplitCount {
		return attributeValue{}, errors.New("invalid update expression")
	}
	left, err := evalUpdateValueExpression(args[0], values, item)
	if err != nil {
		return attributeValue{}, err
	}
	right, err := evalUpdateValueExpression(args[1], values, item)
	if err != nil {
		return attributeValue{}, err
	}
	if !left.hasListType() || !right.hasListType() {
		return attributeValue{}, errors.New("list_append supports only list attributes")
	}
	out := make([]attributeValue, 0, len(left.L)+len(right.L))
	for _, value := range left.L {
		out = append(out, cloneAttributeValue(value))
	}
	for _, value := range right.L {
		out = append(out, cloneAttributeValue(value))
	}
	return attributeValue{L: out}, nil
}

func applyArithmeticUpdateValue(left attributeValue, right attributeValue, op byte) (attributeValue, error) {
	if !left.hasNumberType() || !right.hasNumberType() {
		return attributeValue{}, errors.New("arithmetic update supports only number attributes")
	}
	rightValue := right.numberValue()
	if op == '-' {
		rightValue = "-" + rightValue
	}
	sum, err := addNumericAttributeValues(left.numberValue(), rightValue)
	if err != nil {
		return attributeValue{}, err
	}
	return attributeValue{N: &sum}, nil
}

func findTopLevelArithmeticOperator(expr string) (int, byte, bool) {
	depth := 0
	for i := 0; i < len(expr); i++ {
		depth = nextParenDepth(depth, expr[i])
		if depth != 0 {
			continue
		}
		switch expr[i] {
		case '+', '-':
			if i == 0 {
				continue
			}
			return i, expr[i], true
		}
	}
	return 0, 0, false
}

func parseExpressionFunctionArgs(expr string, funcName string) ([]string, bool, error) {
	prefix := funcName + "("
	if !strings.HasPrefix(strings.ToLower(expr), strings.ToLower(prefix)) || !strings.HasSuffix(expr, ")") {
		return nil, false, nil
	}
	inner := strings.TrimSpace(expr[len(prefix) : len(expr)-1])
	parts, err := splitTopLevelByComma(inner)
	if err != nil {
		return nil, true, errors.New("invalid expression")
	}
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts, true, nil
}

func addAttributeValue(current attributeValue, exists bool, addValue attributeValue) (attributeValue, error) {
	if addValue.hasNumberType() {
		return addNumericUpdateValue(current, exists, addValue)
	}
	if addValue.hasStringSetType() || addValue.hasNumberSetType() || addValue.hasBinarySetType() {
		return addSetUpdateValue(current, exists, addValue)
	}
	return attributeValue{}, errors.New("ADD supports only number or set attributes")
}

func addNumericUpdateValue(current attributeValue, exists bool, addValue attributeValue) (attributeValue, error) {
	if !exists {
		return cloneAttributeValue(addValue), nil
	}
	if !current.hasNumberType() {
		return attributeValue{}, errors.New("ADD supports only number attributes")
	}
	sum, err := addNumericAttributeValues(current.numberValue(), addValue.numberValue())
	if err != nil {
		return attributeValue{}, err
	}
	return attributeValue{N: &sum}, nil
}

func addSetUpdateValue(current attributeValue, exists bool, addValue attributeValue) (attributeValue, error) {
	if !exists {
		return cloneAttributeValue(addValue), nil
	}
	switch {
	case current.hasStringSetType() && addValue.hasStringSetType():
		return attributeValue{SS: mergeStringSet(current.SS, addValue.SS)}, nil
	case current.hasNumberSetType() && addValue.hasNumberSetType():
		return attributeValue{NS: mergeNumberSet(current.NS, addValue.NS)}, nil
	case current.hasBinarySetType() && addValue.hasBinarySetType():
		return attributeValue{BS: mergeBinarySet(current.BS, addValue.BS)}, nil
	default:
		return attributeValue{}, errors.New("ADD supports only matching set attribute types")
	}
}

func mergeStringSet(current []string, add []string) []string {
	out := make([]string, 0, len(current)+len(add))
	seen := make(map[string]struct{}, len(current)+len(add))
	for _, value := range append(append([]string(nil), current...), add...) {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func mergeNumberSet(current []string, add []string) []string {
	out := make([]string, 0, len(current)+len(add))
	seen := make(map[string]struct{}, len(current)+len(add))
	for _, value := range append(append([]string(nil), current...), add...) {
		key := canonicalNumberString(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, value)
	}
	return out
}

func mergeBinarySet(current [][]byte, add [][]byte) [][]byte {
	out := make([][]byte, 0, len(current)+len(add))
	seen := make(map[string]struct{}, len(current)+len(add))
	for _, value := range append(cloneBinarySet(current), add...) {
		key := string(value)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, bytes.Clone(value))
	}
	return out
}

func validateConditionOnItem(expr string, names map[string]string, values map[string]attributeValue, item map[string]attributeValue) error {
	cond, err := replaceNames(expr, names)
	if err != nil {
		return err
	}
	cond = strings.TrimSpace(cond)
	if cond == "" {
		return nil
	}
	ok, err := evalConditionExpression(cond, item, values)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("conditional check failed")
	}
	return nil
}

func evalConditionExpression(expr string, item map[string]attributeValue, values map[string]attributeValue) (bool, error) {
	expr = trimOuterParens(strings.TrimSpace(expr))
	if expr == "" {
		return true, nil
	}
	if ok, handled, err := evalLogicalCondition(expr, "OR", item, values); handled {
		return ok, err
	}
	if ok, handled, err := evalLogicalCondition(expr, "AND", item, values); handled {
		return ok, err
	}
	if rest, ok := trimLeadingKeyword(expr, "NOT"); ok {
		ok, err := evalConditionExpression(rest, item, values)
		if err != nil {
			return false, err
		}
		return !ok, nil
	}
	return evalAtomicCondition(expr, item, values)
}

func trimOuterParens(expr string) string {
	for {
		expr = strings.TrimSpace(expr)
		if !hasOuterParens(expr) {
			return expr
		}
		expr = expr[1 : len(expr)-1]
	}
}

func splitTopLevelByKeyword(expr string, keyword string) []string {
	if expr == "" {
		return nil
	}
	upper := strings.ToUpper(expr)
	target := strings.ToUpper(keyword)
	targetLen := len(target)
	if targetLen == 0 {
		return nil
	}
	depth := 0
	last := 0
	betweenPending := false
	parts := make([]string, 0, splitPartsInitialCapacity)
	for i := 0; i < len(expr); i++ {
		depth = nextParenDepth(depth, expr[i])
		if depth != 0 {
			continue
		}
		if nextIndex, handled, nextPending := consumeBetweenSplitState(expr, upper, keyword, i, betweenPending); handled {
			betweenPending = nextPending
			i = nextIndex
			continue
		}
		if !shouldSplitKeywordAt(expr, upper, target, targetLen, i) {
			continue
		}
		part, ok := trimmedNonEmpty(expr[last:i])
		if !ok {
			return nil
		}
		parts = append(parts, part)
		i += targetLen - 1
		last = i + 1
	}
	return finalizeKeywordSplit(expr[last:], parts)
}

func consumeBetweenSplitState(expr string, upper string, keyword string, index int, betweenPending bool) (int, bool, bool) {
	if !strings.EqualFold(keyword, "AND") {
		return index, false, betweenPending
	}
	if matchesLogicalKeyword(expr, upper, "BETWEEN", index) {
		return index + len("BETWEEN") - 1, true, true
	}
	if betweenPending && matchesLogicalKeyword(expr, upper, "AND", index) {
		return index + len("AND") - 1, true, false
	}
	return index, false, betweenPending
}

func shouldSplitKeywordAt(expr string, upper string, target string, targetLen int, index int) bool {
	return matchesKeywordTokenAt(upper, target, index) &&
		isLogicalKeywordBoundary(expr, index-1) &&
		isLogicalKeywordBoundary(expr, index+targetLen)
}

func matchesLogicalKeyword(expr string, upper string, keyword string, index int) bool {
	return matchesKeywordTokenAt(upper, keyword, index) &&
		isLogicalKeywordBoundary(expr, index-1) &&
		isLogicalKeywordBoundary(expr, index+len(keyword))
}

func evalLogicalCondition(expr string, keyword string, item map[string]attributeValue, values map[string]attributeValue) (bool, bool, error) {
	parts := splitTopLevelByKeyword(expr, keyword)
	if len(parts) == 0 {
		return false, false, nil
	}
	if strings.EqualFold(keyword, "OR") {
		ok, err := evalConditionAny(parts, item, values)
		return ok, true, err
	}
	ok, err := evalConditionAll(parts, item, values)
	return ok, true, err
}

func evalConditionAny(parts []string, item map[string]attributeValue, values map[string]attributeValue) (bool, error) {
	for _, part := range parts {
		ok, err := evalConditionExpression(part, item, values)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

func evalConditionAll(parts []string, item map[string]attributeValue, values map[string]attributeValue) (bool, error) {
	for _, part := range parts {
		ok, err := evalConditionExpression(part, item, values)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func evalAtomicCondition(expr string, item map[string]attributeValue, values map[string]attributeValue) (bool, error) {
	for _, handler := range conditionFunctionHandlers {
		if ok, handled, err := evalNamedConditionFunction(expr, item, values, handler); handled {
			return ok, err
		}
	}
	if ok, handled, err := evalConditionBetween(expr, item, values); handled {
		return ok, err
	}
	if ok, handled, err := evalConditionIn(expr, item, values); handled {
		return ok, err
	}
	return evalConditionComparison(expr, item, values)
}

type conditionFunctionHandler struct {
	name string
	eval func([]string, map[string]attributeValue, map[string]attributeValue) (bool, error)
}

var conditionFunctionHandlers = []conditionFunctionHandler{
	{
		name: "attribute_exists",
		eval: func(args []string, item map[string]attributeValue, _ map[string]attributeValue) (bool, error) {
			return evalAttributeExistsCondition(args, item)
		},
	},
	{
		name: "attribute_not_exists",
		eval: func(args []string, item map[string]attributeValue, _ map[string]attributeValue) (bool, error) {
			return evalAttributeNotExistsCondition(args, item)
		},
	},
	{name: "attribute_type", eval: evalAttributeTypeCondition},
	{name: "begins_with", eval: evalBeginsWithCondition},
	{name: "contains", eval: evalContainsCondition},
}

func evalNamedConditionFunction(
	expr string,
	item map[string]attributeValue,
	values map[string]attributeValue,
	handler conditionFunctionHandler,
) (bool, bool, error) {
	args, ok, err := parseExpressionFunctionArgs(expr, handler.name)
	if err != nil {
		return false, true, err
	}
	if !ok {
		return false, false, nil
	}
	value, err := handler.eval(args, item, values)
	return value, true, err
}

func evalAttributeExistsCondition(args []string, item map[string]attributeValue) (bool, error) {
	if len(args) != 1 {
		return false, errors.New("unsupported condition expression")
	}
	_, found, err := resolveDocumentPath(item, args[0])
	if err != nil {
		return false, err
	}
	return found, nil
}

func evalAttributeNotExistsCondition(args []string, item map[string]attributeValue) (bool, error) {
	ok, err := evalAttributeExistsCondition(args, item)
	if err != nil {
		return false, err
	}
	return !ok, nil
}

func evalAttributeTypeCondition(args []string, item map[string]attributeValue, values map[string]attributeValue) (bool, error) {
	if len(args) != updateSplitCount {
		return false, errors.New("unsupported condition expression")
	}
	value, found, err := resolveDocumentPath(item, args[0])
	if err != nil || !found {
		return false, err
	}
	typeValue, ok := values[strings.TrimSpace(args[1])]
	if !ok || !typeValue.hasStringType() {
		return false, errors.New("unsupported condition expression")
	}
	return dynamoAttributeType(value) == typeValue.stringValue(), nil
}

func evalBeginsWithCondition(args []string, item map[string]attributeValue, values map[string]attributeValue) (bool, error) {
	if len(args) != updateSplitCount {
		return false, errors.New("unsupported condition expression")
	}
	left, found, err := resolveDocumentPath(item, args[0])
	if err != nil || !found {
		return false, err
	}
	right, ok := values[strings.TrimSpace(args[1])]
	if !ok {
		return false, errors.New("missing condition value")
	}
	switch {
	case left.hasStringType() && right.hasStringType():
		return strings.HasPrefix(left.stringValue(), right.stringValue()), nil
	case left.hasBinaryType() && right.hasBinaryType():
		return bytes.HasPrefix(left.B, right.B), nil
	default:
		return false, nil
	}
}

func evalContainsCondition(args []string, item map[string]attributeValue, values map[string]attributeValue) (bool, error) {
	if len(args) != updateSplitCount {
		return false, errors.New("unsupported condition expression")
	}
	left, found, err := resolveDocumentPath(item, args[0])
	if err != nil || !found {
		return false, err
	}
	right, ok := values[strings.TrimSpace(args[1])]
	if !ok {
		return false, errors.New("missing condition value")
	}
	return attributeValueContains(left, right), nil
}

func attributeValueContains(left attributeValue, right attributeValue) bool {
	for _, eval := range attributeValueContainsEvaluators {
		if handled, ok := eval(left, right); handled {
			return ok
		}
	}
	return false
}

type attributeValueContainsEvaluator func(attributeValue, attributeValue) (bool, bool)

var attributeValueContainsEvaluators = []attributeValueContainsEvaluator{
	containsStringAttributeValue,
	containsBinaryAttributeValue,
	containsListAttributeValue,
	containsStringSetAttributeValue,
	containsNumberSetAttributeValue,
	containsBinarySetAttributeValue,
}

func containsStringAttributeValue(left attributeValue, right attributeValue) (bool, bool) {
	if !left.hasStringType() || !right.hasStringType() {
		return false, false
	}
	return true, strings.Contains(left.stringValue(), right.stringValue())
}

func containsBinaryAttributeValue(left attributeValue, right attributeValue) (bool, bool) {
	if !left.hasBinaryType() || !right.hasBinaryType() {
		return false, false
	}
	return true, bytes.Contains(left.B, right.B)
}

func containsListAttributeValue(left attributeValue, right attributeValue) (bool, bool) {
	if !left.hasListType() {
		return false, false
	}
	return true, listContainsAttributeValue(left.L, right)
}

func containsStringSetAttributeValue(left attributeValue, right attributeValue) (bool, bool) {
	if !left.hasStringSetType() || !right.hasStringType() {
		return false, false
	}
	return true, stringSetContains(left.SS, right.stringValue())
}

func containsNumberSetAttributeValue(left attributeValue, right attributeValue) (bool, bool) {
	if !left.hasNumberSetType() || !right.hasNumberType() {
		return false, false
	}
	return true, numberSetContains(left.NS, right.numberValue())
}

func containsBinarySetAttributeValue(left attributeValue, right attributeValue) (bool, bool) {
	if !left.hasBinarySetType() || !right.hasBinaryType() {
		return false, false
	}
	return true, binarySetContains(left.BS, right.B)
}

func listContainsAttributeValue(values []attributeValue, needle attributeValue) bool {
	for _, value := range values {
		if attributeValueEqual(value, needle) {
			return true
		}
	}
	return false
}

func stringSetContains(values []string, needle string) bool {
	return slices.Contains(values, needle)
}

func numberSetContains(values []string, needle string) bool {
	for _, value := range values {
		if cmp, ok := compareNumericAttributeString(value, needle); ok && cmp == 0 {
			return true
		}
	}
	return false
}

func binarySetContains(values [][]byte, needle []byte) bool {
	for _, value := range values {
		if bytes.Equal(value, needle) {
			return true
		}
	}
	return false
}

func evalConditionBetween(expr string, item map[string]attributeValue, values map[string]attributeValue) (bool, bool, error) {
	betweenIndex := findTopLevelKeywordIndex(expr, "BETWEEN")
	if betweenIndex < 0 {
		return false, false, nil
	}
	leftExpr := strings.TrimSpace(expr[:betweenIndex])
	rest := strings.TrimSpace(expr[betweenIndex+len("BETWEEN"):])
	andIndex := findTopLevelKeywordIndex(rest, "AND")
	if andIndex < 0 {
		return false, true, errors.New("unsupported condition expression")
	}
	lowerExpr := strings.TrimSpace(rest[:andIndex])
	upperExpr := strings.TrimSpace(rest[andIndex+len("AND"):])
	left, found, err := resolveConditionOperand(leftExpr, item, values)
	if err != nil || !found {
		return false, true, err
	}
	lower, found, err := resolveConditionOperand(lowerExpr, item, values)
	if err != nil || !found {
		return false, true, err
	}
	upper, found, err := resolveConditionOperand(upperExpr, item, values)
	if err != nil || !found {
		return false, true, err
	}
	return compareAttributeValueSortKey(left, lower) >= 0 && compareAttributeValueSortKey(left, upper) <= 0, true, nil
}

func evalConditionIn(expr string, item map[string]attributeValue, values map[string]attributeValue) (bool, bool, error) {
	inIndex := findTopLevelKeywordIndex(expr, "IN")
	if inIndex < 0 {
		return false, false, nil
	}
	left, parts, err := parseConditionInOperands(expr, inIndex, item, values)
	if err != nil {
		return false, true, err
	}
	ok, err := conditionInListContains(left, parts, item, values)
	return ok, true, err
}

func parseConditionInOperands(expr string, inIndex int, item map[string]attributeValue, values map[string]attributeValue) (attributeValue, []string, error) {
	leftExpr := strings.TrimSpace(expr[:inIndex])
	rest := strings.TrimSpace(expr[inIndex+len("IN"):])
	if !strings.HasPrefix(rest, "(") || !strings.HasSuffix(rest, ")") {
		return attributeValue{}, nil, errors.New("unsupported condition expression")
	}
	left, found, err := resolveConditionOperand(leftExpr, item, values)
	if err != nil || !found {
		return attributeValue{}, nil, err
	}
	parts, err := splitTopLevelByComma(rest[1 : len(rest)-1])
	if err != nil {
		return attributeValue{}, nil, errors.New("unsupported condition expression")
	}
	return left, parts, nil
}

func conditionInListContains(left attributeValue, parts []string, item map[string]attributeValue, values map[string]attributeValue) (bool, error) {
	for _, part := range parts {
		candidate, found, err := resolveConditionOperand(part, item, values)
		if err != nil {
			return false, err
		}
		if found && attributeValueEqual(left, candidate) {
			return true, nil
		}
	}
	return false, nil
}

func evalConditionComparison(expr string, item map[string]attributeValue, values map[string]attributeValue) (bool, error) {
	index, operator, ok := findTopLevelConditionComparator(expr)
	if !ok {
		return false, errors.New("unsupported condition expression")
	}
	left, right, err := resolveConditionComparisonOperands(expr, index, operator, item, values)
	if err != nil {
		return false, err
	}
	return compareConditionValues(operator, left, right)
}

func resolveConditionComparisonOperands(
	expr string,
	index int,
	operator string,
	item map[string]attributeValue,
	values map[string]attributeValue,
) (attributeValue, attributeValue, error) {
	leftExpr := strings.TrimSpace(expr[:index])
	rightExpr := strings.TrimSpace(expr[index+len(operator):])
	left, found, err := resolveConditionOperand(leftExpr, item, values)
	if err != nil || !found {
		return attributeValue{}, attributeValue{}, err
	}
	right, found, err := resolveConditionOperand(rightExpr, item, values)
	if err != nil || !found {
		return attributeValue{}, attributeValue{}, err
	}
	return left, right, nil
}

func compareConditionValues(operator string, left attributeValue, right attributeValue) (bool, error) {
	switch operator {
	case "=":
		return attributeValueEqual(left, right), nil
	case "<>":
		return !attributeValueEqual(left, right), nil
	case "<":
		return compareAttributeValueSortKey(left, right) < 0, nil
	case "<=":
		return compareAttributeValueSortKey(left, right) <= 0, nil
	case ">":
		return compareAttributeValueSortKey(left, right) > 0, nil
	case ">=":
		return compareAttributeValueSortKey(left, right) >= 0, nil
	default:
		return false, errors.New("unsupported condition expression")
	}
}

func resolveConditionOperand(expr string, item map[string]attributeValue, values map[string]attributeValue) (attributeValue, bool, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return attributeValue{}, false, errors.New("unsupported condition expression")
	}
	if args, ok, err := parseExpressionFunctionArgs(expr, "size"); ok || err != nil {
		if err != nil {
			return attributeValue{}, false, err
		}
		return resolveConditionSizeOperand(args, item)
	}
	if strings.HasPrefix(expr, ":") {
		value, ok := values[expr]
		if !ok {
			return attributeValue{}, false, errors.New("missing condition value")
		}
		return cloneAttributeValue(value), true, nil
	}
	value, found, err := resolveDocumentPath(item, expr)
	if err != nil {
		return attributeValue{}, false, err
	}
	return value, found, nil
}

func resolveConditionSizeOperand(args []string, item map[string]attributeValue) (attributeValue, bool, error) {
	if len(args) != 1 {
		return attributeValue{}, false, errors.New("unsupported condition expression")
	}
	value, found, err := resolveDocumentPath(item, args[0])
	if err != nil || !found {
		return attributeValue{}, false, err
	}
	size := attributeValueSize(value)
	sizeString := strconv.Itoa(size)
	return attributeValue{N: &sizeString}, true, nil
}

func attributeValueSize(value attributeValue) int {
	switch {
	case value.hasStringType():
		return len(value.stringValue())
	case value.hasBinaryType():
		return len(value.B)
	case value.hasStringSetType():
		return len(value.SS)
	case value.hasNumberSetType():
		return len(value.NS)
	case value.hasBinarySetType():
		return len(value.BS)
	case value.hasListType():
		return len(value.L)
	case value.hasMapType():
		return len(value.M)
	default:
		return 0
	}
}

func findTopLevelKeywordIndex(expr string, keyword string) int {
	upper := strings.ToUpper(expr)
	target := strings.ToUpper(keyword)
	depth := 0
	for i := 0; i < len(expr); i++ {
		depth = nextParenDepth(depth, expr[i])
		if depth != 0 || !matchesKeywordTokenAt(upper, target, i) {
			continue
		}
		if !isLogicalKeywordBoundary(expr, i-1) || !isLogicalKeywordBoundary(expr, i+len(target)) {
			continue
		}
		return i
	}
	return -1
}

func trimLeadingKeyword(expr string, keyword string) (string, bool) {
	upper := strings.ToUpper(strings.TrimSpace(expr))
	keyword = strings.ToUpper(keyword)
	if !strings.HasPrefix(upper, keyword) {
		return "", false
	}
	trimmed := strings.TrimSpace(expr)
	if !isLogicalKeywordBoundary(trimmed, len(keyword)) {
		return "", false
	}
	return strings.TrimSpace(trimmed[len(keyword):]), true
}

func findTopLevelConditionComparator(expr string) (int, string, bool) {
	operators := []string{"<>", "<=", ">=", "=", "<", ">"}
	depth := 0
	for i := 0; i < len(expr); i++ {
		depth = nextParenDepth(depth, expr[i])
		if depth != 0 {
			continue
		}
		for _, operator := range operators {
			if strings.HasPrefix(expr[i:], operator) {
				return i, operator, true
			}
		}
	}
	return 0, "", false
}

func dynamoAttributeType(value attributeValue) string {
	kind, count := detectAttributeValueKind(value)
	if count != 1 {
		return ""
	}
	return string(kind)
}

func hasOuterParens(expr string) bool {
	if len(expr) < 2 || expr[0] != '(' || expr[len(expr)-1] != ')' {
		return false
	}
	depth := 0
	for i := 0; i < len(expr); i++ {
		depth = nextParenDepth(depth, expr[i])
		if depth == 0 && i != len(expr)-1 {
			return false
		}
		if depth < 0 {
			return false
		}
	}
	return depth == 0
}

func nextParenDepth(depth int, ch byte) int {
	switch ch {
	case '(':
		return depth + 1
	case ')':
		return depth - 1
	default:
		return depth
	}
}

func matchesKeywordTokenAt(upperExpr string, target string, pos int) bool {
	end := pos + len(target)
	if end > len(upperExpr) {
		return false
	}
	return upperExpr[pos:end] == target
}

func isLogicalKeywordBoundary(s string, pos int) bool {
	if pos < 0 || pos >= len(s) {
		return true
	}
	ch := s[pos]
	// Keep identifier-style characters as token characters so expressions like
	// "MY_AND_VAR" or "a-OR-b" are not split at logical keyword substrings.
	if isExpressionAttributeNameByte(ch) {
		return false
	}
	return true
}

func trimmedNonEmpty(s string) (string, bool) {
	trimmed := strings.TrimSpace(s)
	return trimmed, trimmed != ""
}

func finalizeKeywordSplit(tailExpr string, parts []string) []string {
	if len(parts) == 0 {
		return nil
	}
	tail, ok := trimmedNonEmpty(tailExpr)
	if !ok {
		return nil
	}
	return append(parts, tail)
}
