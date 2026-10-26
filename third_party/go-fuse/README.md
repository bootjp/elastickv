# Local go-fuse bridge

This directory contains the `fuse` and `splice` packages from
`github.com/hanwen/go-fuse/v2` v2.12.0, under its BSD license.

The local change adds `ReplyAttemptObserver` and calls it immediately after
`Server.handleRequest` attempts to write a response to the FUSE device.
Elastickv uses that boundary to retire terminal filesystem operation IDs.
The callback is skipped when go-fuse suppresses the reply.

When updating go-fuse, preserve the notification after the device write and
the suppressed-reply condition, then rerun the kernel FUSE tests.
