package adapter

import "time"

// redisShutdownTimeout bounds how long Stop waits for the commands still
// being executed once the listener and connections are closed. redcon ends
// Serve as soon as the listener closes and does not join its handler
// goroutines, so without this wait the errgroup task in main would finish,
// and the stores be closed, underneath a handler still dispatching (a SETEX
// waiting on Raft, for example). A variable so tests can shorten it.
var redisShutdownTimeout = 10 * time.Second
