export function Loading({ label = 'Loading…' }: { label?: string }) {
  return (
    <div className="empty-msg">
      <span className="spinner"></span> {label}
    </div>
  );
}

export function Empty({ label }: { label: string }) {
  return <div className="empty-msg">{label}</div>;
}

export function ErrorMsg({ error }: { error: Error }) {
  return <div className="empty-msg">Error: {error.message}</div>;
}
