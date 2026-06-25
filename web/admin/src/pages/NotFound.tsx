import { Link } from "react-router-dom";

export function NotFoundPage() {
  return (
    <div className="card text-sm">
      <div className="font-semibold">Page not found</div>
      <p className="text-muted mt-1">
        <Link to="/" className="text-accent hover:underline">Back to overview</Link>
      </p>
    </div>
  );
}
