import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { App } from "./App";
import "./styles.css";

const container = document.getElementById("root");
if (!container) {
  throw new Error("admin SPA: #root not found");
}

createRoot(container).render(
  <StrictMode>
    {/* basename keeps react-router aligned with the /admin/* prefix the
        Go router serves index.html for. Without it, in-app navigation
        would build URLs at the document root and bypass the SPA. */}
    <BrowserRouter basename="/admin">
      <App />
    </BrowserRouter>
  </StrictMode>,
);
