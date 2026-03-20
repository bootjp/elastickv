package proxy

import (
	"crypto/sha1" // #nosec G505 -- Redis EVALSHA specifies SHA1 script digests.
	"encoding/hex"
	"errors"
	"strings"

	"github.com/redis/go-redis/v9"
)

const (
	cmdEval      = "EVAL"
	cmdEvalRO    = "EVAL_RO"
	cmdEvalSHA   = "EVALSHA"
	cmdEvalSHARO = "EVALSHA_RO"
	cmdScript    = "SCRIPT"

	minScriptSubcommandArgs = 2
	scriptLoadArgIndex      = 2
	minEvalSHAArgs          = 2

	// maxScriptCacheSize is the maximum number of scripts retained in the
	// proxy-side script cache. When the limit is reached, the oldest entry
	// (by insertion order) is evicted to prevent unbounded memory growth.
	maxScriptCacheSize = 512
)

func (d *DualWriter) rememberScript(cmd string, args [][]byte) {
	upper := strings.ToUpper(cmd)

	switch upper {
	case cmdEval, cmdEvalRO:
		if len(args) > 1 {
			d.storeScript(string(args[1]))
		}
	case cmdScript:
		if len(args) < minScriptSubcommandArgs {
			return
		}
		switch strings.ToUpper(string(args[1])) {
		case "LOAD":
			if len(args) > scriptLoadArgIndex {
				d.storeScript(string(args[scriptLoadArgIndex]))
			}
		case "FLUSH":
			d.clearScripts()
		}
	}
}

func (d *DualWriter) storeScript(script string) {
	sha := scriptSHA(script)

	d.scriptMu.Lock()
	defer d.scriptMu.Unlock()

	if _, exists := d.scripts[sha]; !exists {
		// Evict the oldest entry when at capacity.
		// scriptOrder and scripts always have the same length, so the guard is
		// sufficient; the additional len(scriptOrder) check is not needed.
		if len(d.scripts) >= maxScriptCacheSize {
			oldest := d.scriptOrder[0]
			d.scriptOrder = d.scriptOrder[1:]
			delete(d.scripts, oldest)
		}
		// Append to the end so that eviction follows strict FIFO insertion order.
		// Re-storing an already-cached script does not move its position; the
		// entry retains its original eviction priority, which is acceptable for
		// this use-case because script bodies are immutable (same SHA ⇒ same body).
		d.scriptOrder = append(d.scriptOrder, sha)
	}
	d.scripts[sha] = script
}

func (d *DualWriter) clearScripts() {
	d.scriptMu.Lock()
	defer d.scriptMu.Unlock()
	clear(d.scripts)
	d.scriptOrder = d.scriptOrder[:0]
}

func (d *DualWriter) lookupScript(sha string) (string, bool) {
	d.scriptMu.RLock()
	defer d.scriptMu.RUnlock()
	script, ok := d.scripts[strings.ToLower(sha)]
	return script, ok
}

func (d *DualWriter) evalFallbackArgs(cmd string, iArgs []any) ([]any, bool) {
	upper := strings.ToUpper(cmd)
	if upper != cmdEvalSHA && upper != cmdEvalSHARO {
		return nil, false
	}
	if len(iArgs) < minEvalSHAArgs {
		return nil, false
	}

	sha := stringArg(iArgs[1])
	script, ok := d.lookupScript(sha)
	if !ok {
		return nil, false
	}

	// Preserve the read-only semantics: EVALSHA_RO falls back to EVAL_RO.
	fallbackCmd := cmdEval
	if upper == cmdEvalSHARO {
		fallbackCmd = cmdEvalRO
	}

	fallback := make([]any, len(iArgs))
	fallback[0] = []byte(fallbackCmd)
	fallback[1] = []byte(script)
	copy(fallback[2:], iArgs[2:])
	return fallback, true
}

func isNoScriptError(err error) bool {
	if err == nil {
		return false
	}
	var redisErr redis.Error
	if errors.As(err, &redisErr) {
		return strings.HasPrefix(redisErr.Error(), "NOSCRIPT ")
	}
	return strings.HasPrefix(err.Error(), "NOSCRIPT ")
}

func scriptSHA(script string) string {
	// #nosec G401 -- Redis EVALSHA uses SHA1 digests by protocol.
	sum := sha1.Sum([]byte(script))
	return hex.EncodeToString(sum[:])
}

func stringArg(arg any) string {
	switch v := arg.(type) {
	case []byte:
		return strings.ToLower(string(v))
	case string:
		return strings.ToLower(v)
	default:
		return strings.ToLower(string(argsToBytes([]any{arg})[0]))
	}
}
