//go:build windows

package kek

// kekOpenExtraFlags is 0 on Windows: there is no portable O_NONBLOCK,
// and the FIFO-block scenario it guards against on unix does not
// apply to a regular Windows file path. The IsRegular() check on
// the fd's stat is enough to reject pipes, devices, and directories.
const kekOpenExtraFlags = 0
