package proxy

// go-redis decodes RESP simple strings and bulk strings into the same Go
// string type. In COMMAND DOCS that distinction matters to clients:
// redis-cli requires argument flags to be simple strings.
// Reconstruct those fields from the reply schema when forwarding DOCS.
func writeCommandDocsResponse(w respWriter, resp any, err error) {
	if err != nil {
		writeResponse(w, nil, err)
		return
	}
	writeCommandDocsValue(w, resp)
}

func writeCommandDocsValue(w respWriter, value any) {
	switch v := value.(type) {
	case []any:
		writeCommandDocsArray(w, v)
	case string:
		// All other strings in COMMAND DOCS are bulk strings, including
		// text that happens to match a common status such as "OK".
		w.WriteBulkString(v)
	default:
		writeRedisValue(w, v)
	}
}

func writeCommandDocsArray(w respWriter, values []any) {
	w.WriteArray(len(values))
	for i := 0; i < len(values); i++ {
		// RESP2 maps are alternating key/value arrays. Redis emits both
		// argument flags and command doc_flags with addReplyStatus.
		if i%2 == 0 && i+1 < len(values) && writeCommandDocsFlagPair(w, values[i], values[i+1]) {
			i++
			continue
		}
		writeCommandDocsValue(w, values[i])
	}
}

func writeCommandDocsFlagPair(w respWriter, keyValue, value any) bool {
	key, ok := keyValue.(string)
	if !ok || (key != "flags" && key != "doc_flags") {
		return false
	}
	flags, ok := value.([]any)
	return ok && writeCommandDocsFlags(w, key, flags)
}

// Return false without writing if the value is not a string-only flag set.
// This leaves malformed or future non-string values to the normal writer.
func writeCommandDocsFlags(w respWriter, key string, flags []any) bool {
	strings := make([]string, len(flags))
	for i, flag := range flags {
		value, ok := flag.(string)
		if !ok {
			return false
		}
		strings[i] = value
	}
	w.WriteBulkString(key)
	w.WriteArray(len(flags))
	for _, flag := range strings {
		w.WriteString(flag)
	}
	return true
}
