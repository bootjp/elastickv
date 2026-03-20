package proxy

import (
	"crypto/sha1" // #nosec G505 -- Redis EVALSHA specifies SHA1 script digests.
	"encoding/hex"
	"errors"
	"strings"

	"github.com/redis/go-redis/v9"
)

const (
	cmdEval    = "EVAL"
	cmdEvalSHA = "EVALSHA"
	cmdScript  = "SCRIPT"

	minScriptSubcommandArgs = 2
	scriptLoadArgIndex      = 2
	minEvalSHAArgs          = 2
)

func (d *DualWriter) rememberScript(cmd string, args [][]byte) {
	upper := strings.ToUpper(cmd)

	switch upper {
	case cmdEval, "EVAL_RO":
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
	d.scripts[sha] = script
}

func (d *DualWriter) clearScripts() {
	d.scriptMu.Lock()
	defer d.scriptMu.Unlock()
	clear(d.scripts)
}

func (d *DualWriter) lookupScript(sha string) (string, bool) {
	d.scriptMu.RLock()
	defer d.scriptMu.RUnlock()
	script, ok := d.scripts[strings.ToLower(sha)]
	return script, ok
}

func (d *DualWriter) evalFallbackArgs(cmd string, iArgs []any) ([]any, bool) {
	upper := strings.ToUpper(cmd)
	if upper != cmdEvalSHA && upper != "EVALSHA_RO" {
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

	fallback := make([]any, len(iArgs))
	fallback[0] = []byte(cmdEval)
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
