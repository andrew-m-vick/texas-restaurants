// Generic placeholder so the router compiles before every page is ported.
// Each remaining page should be replaced with a real component — see
// frontend/README.md for the patterns to follow.
export default function Stub({ name, hint }: { name: string; hint: string }) {
  return (
    <div className="card">
      <h1>{name}</h1>
      <p className="muted">Not yet ported to React. {hint}</p>
    </div>
  );
}
