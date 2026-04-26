package admin

import (
	"embed"
	"errors"
	"io/fs"
)

// distFS holds the Vite build output for the admin SPA. The directory
// is populated by `npm run build` under web/admin/, which writes its
// output straight into internal/admin/dist (see web/admin/vite.config.ts).
//
// We embed `dist` as a directory rather than a glob so that adding a new
// asset under dist/assets/ does not require touching this file. The
// `all:` prefix is intentional — Vite occasionally emits files whose
// names start with `.` (sourcemaps, tooling artefacts), and the default
// embed selector would silently drop them.
//
//go:embed all:dist
var distFS embed.FS

// StaticFS returns the io/fs.FS that backs /admin/assets/* and the SPA
// fallback. The returned FS is rooted at the embedded `dist` directory,
// so `index.html` resolves to `dist/index.html` and assets resolve to
// `dist/assets/*` — matching the pathing the Router expects.
//
// When the SPA bundle has not been built (only the placeholder
// index.html that ships with the repo is present), the FS is still
// returned: the placeholder renders a short message telling the
// operator how to populate the bundle. Returning nil here would have
// the router answer with JSON 404, which is more confusing than a
// page that explains itself.
func StaticFS() (fs.FS, error) {
	sub, err := fs.Sub(distFS, "dist")
	if err != nil {
		return nil, errors.Join(errors.New("admin: open embedded dist subtree"), err)
	}
	return sub, nil
}
