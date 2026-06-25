import { beforeEach, describe, expect, it, vi } from "vitest";
import { ApiError, api, apiFetch, encodeAdminItemKey } from "./client";

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

describe("apiFetch", () => {
  beforeEach(() => {
    window.history.replaceState({}, "", "/admin/");
  });

  it("builds admin API URLs with query parameters and same-origin credentials", async () => {
    const fetchMock = mockFetch();
    fetchMock.mockResolvedValueOnce(jsonResponse({ tables: [] }));

    await api.listTables("cursor 1");

    expect(fetchMock).toHaveBeenCalledWith(
      "/admin/api/v1/dynamo/tables?next_token=cursor+1",
      expect.objectContaining({
        method: "GET",
        credentials: "same-origin",
        headers: { Accept: "application/json" },
      }),
    );
  });

  it("adds the CSRF cookie value to mutating JSON requests", async () => {
    const fetchMock = mockFetch();
    document.cookie = "admin_csrf=token%20value; path=/";
    fetchMock.mockResolvedValueOnce(new Response(null, { status: 204 }));

    await apiFetch<void>("/auth/logout", { method: "POST", body: { reason: "test" } });

    expect(fetchMock).toHaveBeenCalledWith(
      "/admin/api/v1/auth/logout",
      expect.objectContaining({
        method: "POST",
        credentials: "same-origin",
        body: JSON.stringify({ reason: "test" }),
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
          "X-Admin-CSRF": "token value",
        },
      }),
    );
  });

  it("surfaces structured JSON errors as ApiError", async () => {
    const fetchMock = mockFetch();
    fetchMock.mockResolvedValueOnce(
      jsonResponse({ error: "forbidden", message: "read only" }, { status: 403 }),
    );

    await expect(apiFetch("/dynamo/tables", { method: "POST" })).rejects.toMatchObject({
      status: 403,
      code: "forbidden",
      message: "read only",
    });
  });

  it("rejects successful non-JSON responses before callers parse them", async () => {
    const fetchMock = mockFetch();
    fetchMock.mockResolvedValueOnce(
      new Response("<html></html>", {
        status: 200,
        headers: { "Content-Type": "text/html" },
      }),
    );

    await expect(apiFetch("/cluster")).rejects.toMatchObject({
      status: 200,
      code: "non_json_response",
    });
  });
});

describe("admin API helpers", () => {
  it("encodes DynamoDB item keys with unpadded base64-url JSON", async () => {
    expect(encodeAdminItemKey({ pk: { S: "a/b" } })).toBe("eyJwayI6eyJTIjoiYS9iIn19");

    const fetchMock = mockFetch();
    fetchMock.mockResolvedValueOnce(jsonResponse({ attributes: {} }));

    await api.getItem("table/one", { pk: { S: "a/b" } });

    const [url] = fetchMock.mock.calls[0] ?? [];
    expect(url).toBe("/admin/api/v1/dynamo/tables/table%2Fone/items/eyJwayI6eyJTIjoiYS9iIn19");
  });

  it("throws ApiError instances with status and code fields", () => {
    const err = new ApiError(429, "rate_limited", "too many attempts");

    expect(err).toBeInstanceOf(Error);
    expect(err.status).toBe(429);
    expect(err.code).toBe("rate_limited");
    expect(err.message).toBe("too many attempts");
  });
});
