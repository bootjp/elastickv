import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it, vi } from "vitest";
import { AuthProvider } from "../auth";
import { SqsListPage } from "./SqsList";

function setup(role = "full", fail = false) {
  sessionStorage.setItem("elastickv-admin.session.v1", JSON.stringify({
    role, expires_at: new Date(Date.now() + 60_000).toISOString(),
  }));
  let names: string[] = [];
  const fetchMock = vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
    if (init?.method === "POST") {
      if (fail) return new Response(JSON.stringify({ error: "invalid_request", message: "Queue already exists" }),
        { status: 400, headers: { "Content-Type": "application/json" } });
      const body = JSON.parse(String(init.body)) as { name: string };
      names = [body.name];
      return new Response(JSON.stringify({ name: body.name }),
        { status: 201, headers: { "Content-Type": "application/json" } });
    }
    return new Response(JSON.stringify({ queues: names }),
      { headers: { "Content-Type": "application/json" } });
  });
  vi.stubGlobal("fetch", fetchMock);
  render(<MemoryRouter><AuthProvider><SqsListPage /></AuthProvider></MemoryRouter>);
  return fetchMock;
}

describe("SQS queue creation", () => {
  it("creates a Standard queue and refreshes the list", async () => {
    const fetchMock = setup();
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Create queue" }));
    await user.type(screen.getByLabelText("Queue name"), "orders");
    await user.click(screen.getByRole("button", { name: /^Create$/ }));
    expect(await screen.findByRole("link", { name: "orders" })).toBeInTheDocument();
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledWith("/admin/api/v1/sqs/queues",
      expect.objectContaining({ method: "POST", body: JSON.stringify({ name: "orders", attributes: {} }) }));
  });

  it("validates FIFO names and sends FIFO attributes", async () => {
    const fetchMock = setup();
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Create queue" }));
    await user.selectOptions(screen.getByLabelText("Queue type"), "fifo");
    await user.type(screen.getByLabelText("Queue name"), "orders");
    await user.click(screen.getByRole("button", { name: /^Create$/ }));
    expect(await screen.findByRole("alert")).toHaveTextContent("FIFO names must end in .fifo");
    expect(fetchMock.mock.calls.some(([, init]) => init?.method === "POST")).toBe(false);
    await user.type(screen.getByLabelText("Queue name"), ".fifo");
    await user.click(screen.getByLabelText("Content-based deduplication"));
    await user.click(screen.getByRole("button", { name: /^Create$/ }));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    expect(fetchMock).toHaveBeenCalledWith("/admin/api/v1/sqs/queues",
      expect.objectContaining({ method: "POST", body: JSON.stringify({
        name: "orders.fifo", attributes: { FifoQueue: "true", ContentBasedDeduplication: "true" },
      }) }));
  });

  it("keeps the form open and displays server errors", async () => {
    setup("full", true);
    const user = userEvent.setup();
    await user.click(screen.getByRole("button", { name: "Create queue" }));
    await user.type(screen.getByLabelText("Queue name"), "orders");
    await user.click(screen.getByRole("button", { name: /^Create$/ }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Queue already exists");
    expect(screen.getByRole("dialog")).toBeInTheDocument();
  });

  it("hides creation for read-only users", async () => {
    setup("read_only");
    await screen.findByText("No queues yet.");
    expect(screen.queryByRole("button", { name: "Create queue" })).not.toBeInTheDocument();
  });
});
