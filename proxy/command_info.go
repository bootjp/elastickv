package proxy

const (
	commandInfoFlagsIndex       = 2
	commandInfoCategoriesIndex  = 6
	commandInfoTipsIndex        = 7
	commandInfoKeySpecsIndex    = 8
	commandInfoSubcommandsIndex = 9
	keyAndFlagsPairWidth        = 2
)

// COMMAND INFO uses RESP2 simple strings for command flags, ACL categories,
// tips and key-spec flags. go-redis decodes both string types as Go strings,
// so the parsed proxy restores these fields from the documented reply shape.
func writeCommandInfoResponse(w respWriter, resp any, err error) {
	if err != nil {
		writeResponse(w, nil, err)
		return
	}
	entries, ok := resp.([]any)
	if !ok {
		writeRedisValue(w, resp)
		return
	}
	w.WriteArray(len(entries))
	for _, entry := range entries {
		writeCommandInfoEntry(w, entry)
	}
}

func writeCommandInfoEntry(w respWriter, value any) {
	entry, ok := value.([]any)
	if !ok {
		writeRedisValue(w, value)
		return
	}
	w.WriteArray(len(entry))
	for i, field := range entry {
		switch i {
		case commandInfoFlagsIndex, commandInfoCategoriesIndex, commandInfoTipsIndex:
			writeStatusStringArray(w, field)
		case commandInfoKeySpecsIndex:
			writeCommandKeySpecs(w, field)
		case commandInfoSubcommandsIndex:
			writeCommandInfoResponse(w, field, nil)
		default:
			writeRedisValue(w, field)
		}
	}
}

func writeCommandKeySpecs(w respWriter, value any) {
	values, ok := value.([]any)
	if !ok {
		writeRedisValue(w, value)
		return
	}
	w.WriteArray(len(values))
	for _, item := range values {
		writeCommandKeySpecValue(w, item)
	}
}

func writeCommandKeySpecValue(w respWriter, value any) {
	values, ok := value.([]any)
	if !ok {
		writeRedisValue(w, value)
		return
	}
	w.WriteArray(len(values))
	for i := 0; i < len(values); i++ {
		if i%2 == 0 && i+1 < len(values) && writeCommandDocsFlagPair(w, values[i], values[i+1]) {
			i++
			continue
		}
		writeCommandKeySpecValue(w, values[i])
	}
}

func writeCommandGetKeysAndFlagsResponse(w respWriter, resp any, err error) {
	if err != nil {
		writeResponse(w, nil, err)
		return
	}
	entries, ok := resp.([]any)
	if !ok {
		writeRedisValue(w, resp)
		return
	}
	w.WriteArray(len(entries))
	for _, entry := range entries {
		pair, ok := entry.([]any)
		if !ok || len(pair) != keyAndFlagsPairWidth {
			writeRedisValue(w, entry)
			continue
		}
		w.WriteArray(keyAndFlagsPairWidth)
		writeRedisValue(w, pair[0])
		writeStatusStringArray(w, pair[1])
	}
}

func writeStatusStringArray(w respWriter, value any) {
	values, ok := value.([]any)
	if !ok {
		writeRedisValue(w, value)
		return
	}
	strings := make([]string, len(values))
	for i, item := range values {
		itemString, ok := item.(string)
		if !ok {
			writeRedisValue(w, value)
			return
		}
		strings[i] = itemString
	}
	w.WriteArray(len(values))
	for _, item := range strings {
		w.WriteString(item)
	}
}
