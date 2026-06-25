import "@testing-library/jest-dom/vitest";
import { cleanup } from "@testing-library/react";
import { afterEach, vi } from "vitest";

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  sessionStorage.clear();
  document.cookie = "admin_csrf=; Max-Age=0; path=/admin";
  document.cookie = "admin_csrf=; Max-Age=0; path=/";
});
