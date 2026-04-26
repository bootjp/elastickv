import { NavLink, Outlet, useNavigate } from "react-router-dom";
import { useAuth } from "../auth";

const navItems: { to: string; label: string; end?: boolean }[] = [
  { to: "/", label: "Overview", end: true },
  { to: "/dynamo", label: "DynamoDB" },
  { to: "/sqs", label: "SQS" },
  { to: "/s3", label: "S3" },
];

export function Layout() {
  const { session, logout } = useAuth();
  const navigate = useNavigate();

  const onSignOut = async () => {
    await logout();
    navigate("/login", { replace: true });
  };

  return (
    <div className="min-h-screen flex flex-col">
      <header className="border-b border-border bg-surface-2">
        <div className="mx-auto max-w-6xl px-4 py-3 flex items-center gap-6">
          <div className="font-semibold text-lg">elastickv admin</div>
          <nav className="flex gap-1 text-sm">
            {navItems.map((item) => (
              <NavLink
                key={item.to}
                to={item.to}
                end={item.end}
                className={({ isActive }) =>
                  `px-3 py-1.5 rounded-md transition-colors ${
                    isActive
                      ? "bg-accent text-white"
                      : "text-muted hover:text-ink hover:bg-surface"
                  }`
                }
              >
                {item.label}
              </NavLink>
            ))}
          </nav>
          <div className="ml-auto flex items-center gap-3 text-sm">
            {session && (
              <>
                <span className={session.role === "full" ? "pill-accent" : "pill-muted"}>
                  {session.role === "full" ? "full access" : "read only"}
                </span>
                <button type="button" className="btn-secondary" onClick={onSignOut}>
                  Sign out
                </button>
              </>
            )}
          </div>
        </div>
      </header>
      <main className="flex-1">
        <div className="mx-auto max-w-6xl px-4 py-6">
          <Outlet />
        </div>
      </main>
    </div>
  );
}
