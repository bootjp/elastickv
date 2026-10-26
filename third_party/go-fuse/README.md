# Local go-fuse bridge

This directory contains the `fuse` and `splice` packages from
`github.com/hanwen/go-fuse/v2` v2.12.0, under its BSD license.

The local change adds `ReplyAttemptObserver` immediately after
`Server.handleRequest` attempts to write a response to the FUSE device.
`ReplySuppressedObserver` runs instead when go-fuse has decided to suppress
the reply. Elastickv uses either terminal boundary to retire filesystem
operation IDs after resolving their journal outcome.

When updating go-fuse, preserve both notification boundaries, then rerun the
kernel FUSE tests.
