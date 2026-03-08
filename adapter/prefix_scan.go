package adapter

import "bytes"

const prefixEndMaxByte = 0xFF

func prefixScanEnd(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}

	end := bytes.Clone(prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] == prefixEndMaxByte {
			continue
		}
		end[i]++
		return end[:i+1]
	}

	return nil
}
