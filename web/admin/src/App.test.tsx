import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { App } from "./App";
import { AuthProvider } from "./auth";
import { LoginPage } from "./pages/Login";

const storageKey = "elastickv-admin.session.v1";

function jsonResponse(body: unknown, init: ResponseInit = {}) {
  return new Response(JSON.stringify(body), {
    ...init,
    headers: {
      "Content-Type": "application/json",
      ...init.headers,
    },
  });
}

function mockFetch() {
  const fetchMock = vi.fn<(input: RequestInfo | URL, init?: RequestInit) => Promise<Response>>();
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

function persistSession(role: "read_only" | "full" = "full") {
  sessionStorage.setItem(
    storageKey,
    JSON.stringify({
      role,
      expires_at: new Date(Date.now() + 60_000).toISOString(),
    }),
  );
}

describe("App routing", () => {
  beforeEach(() => {
    window.history.replaceState({}, "", "/admin/");
  });

  it("redirects unauthenticated routes to the login page", async () => {
    render(
      <MemoryRouter initialEntries={["/sqs?filter=active"]}>
        <App />
      </MemoryRouter>,
    );

    expect(await screen.findByRole("button", { name: "Sign in" })).toBeInTheDocument();
    expect(screen.getByLabelText("Access key")).toBeInTheDocument();
    expect(screen.getByLabelText("Secret key")).toBeInTheDocument();
  });

  it("renders the authenticated dashboard from API data", async () => {
    persistSession("read_only");
    const fetchMock = mockFetch();
    fetchMock.mockImplementation(async (input) => {
      const path = String(input);
      if (path === "/admin/api/v1/cluster") {
        return jsonResponse({
          node_id: "node-a",
          version: "test-build",
          timestamp: "2026-06-25T00:00:00Z",
          groups: [
            {
              group_id: 1,
              leader_id: "node-a",
              members: ["node-a", "node-b"],
              is_leader: true,
            },
          ],
        });
      }
      if (path === "/admin/api/v1/dynamo/tables") {
        return jsonResponse({ tables: ["orders", "users"] });
      }
      if (path === "/admin/api/v1/s3/buckets") {
        return jsonResponse({ buckets: [{ bucket_name: "logs" }] });
      }
      return jsonResponse({ error: "not_found", message: path }, { status: 404 });
    });

    render(
      <MemoryRouter initialEntries={["/"]}>
        <App />
      </MemoryRouter>,
    );

    expect((await screen.findAllByText("node-a")).length).toBeGreaterThanOrEqual(2);
    expect(screen.getByText("test-build")).toBeInTheDocument();
    expect(screen.getByText("read only")).toBeInTheDocument();
    expect(screen.getByText("#1")).toBeInTheDocument();
    expect(screen.getByText("node-a, node-b")).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledWith(
      "/admin/api/v1/cluster",
      expect.objectContaining({ credentials: "same-origin" }),
    );
  });
});

describe("LoginPage", () => {
  beforeEach(() => {
    window.history.replaceState({}, "", "/admin/login");
  });

  it("trims pasted credentials and returns to the requested route after login", async () => {
    const user = userEvent.setup();
    const fetchMock = mockFetch();
    fetchMock.mockResolvedValueOnce(
      jsonResponse({
        role: "full",
        expires_at: new Date(Date.now() + 60_000).toISOString(),
      }),
    );

    render(
      <MemoryRouter initialEntries={[{ pathname: "/login", state: { from: "/keyviz" } }]}>
        <AuthProvider>
          <Routes>
            <Route path="/login" element={<LoginPage />} />
            <Route path="/keyviz" element={<div>KeyViz target</div>} />
          </Routes>
        </AuthProvider>
      </MemoryRouter>,
    );

    await user.type(screen.getByLabelText("Access key"), "  AKIA_TEST  ");
    await user.type(screen.getByLabelText("Secret key"), "  SECRET_TEST\n");
    await user.click(screen.getByRole("button", { name: "Sign in" }));

    expect(await screen.findByText("KeyViz target")).toBeInTheDocument();
    const [, init] = fetchMock.mock.calls[0] ?? [];
    expect(JSON.parse(String(init?.body))).toEqual({
      access_key: "AKIA_TEST",
      secret_key: "SECRET_TEST",
    });
  });

  it("shows a specific error message for invalid credentials", async () => {
    const user = userEvent.setup();
    const fetchMock = mockFetch();
    fetchMock.mockResolvedValueOnce(
      jsonResponse({ error: "unauthorized", message: "bad credentials" }, { status: 401 }),
    );

    render(
      <MemoryRouter initialEntries={["/login"]}>
        <AuthProvider>
          <Routes>
            <Route path="/login" element={<LoginPage />} />
          </Routes>
        </AuthProvider>
      </MemoryRouter>,
    );

    await user.type(screen.getByLabelText("Access key"), "bad");
    await user.type(screen.getByLabelText("Secret key"), "bad");
    await user.click(screen.getByRole("button", { name: "Sign in" }));

    await waitFor(() => {
      expect(screen.getByRole("alert")).toHaveTextContent("Invalid access key or secret key.");
    });
  });
});
