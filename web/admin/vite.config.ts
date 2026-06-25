import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { fileURLToPath, URL } from "node:url";

// SPA is mounted at /admin by the Go router (internal/admin/router.go).
// Vite's `base` controls both the asset URLs in index.html and the dev
// server path; `/admin/` keeps both consistent with what the embed
// handler serves at runtime.
export default defineConfig({
  plugins: [react()],
  base: "/admin/",
  build: {
    // Build output goes straight into the Go embed directory so a
    // `npm run build` followed by `go build` produces a single binary
    // with the latest SPA bundle. Using fileURLToPath instead of
    // __dirname keeps the config valid under ESM without pulling in
    // @types/node just for a string concat.
    outDir: fileURLToPath(new URL("../../internal/admin/dist", import.meta.url)),
    emptyOutDir: true,
    sourcemap: false,
    // Keep asset paths under dist/assets so they map onto the
    // /admin/assets/* route the Go router exposes.
    assetsDir: "assets",
  },
  server: {
    port: 5173,
    proxy: {
      // During `npm run dev`, forward API + auth calls to a local
      // elastickv admin listener. Adjust the target if running on a
      // different host/port.
      "/admin/api": "http://127.0.0.1:8080",
      "/admin/healthz": "http://127.0.0.1:8080",
    },
  },
});
