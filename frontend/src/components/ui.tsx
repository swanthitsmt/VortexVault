import { PropsWithChildren } from "react";

type BaseProps = PropsWithChildren<{ className?: string }>;

function join(...parts: Array<string | undefined>): string {
  return parts.filter(Boolean).join(" ");
}

export function Card({ className, children }: BaseProps) {
  return <section className={join("ui-card", className)}>{children}</section>;
}

export function CardTitle({ className, children }: BaseProps) {
  return <h3 className={join("ui-card-title", className)}>{children}</h3>;
}

export function CardBody({ className, children }: BaseProps) {
  return <div className={join("ui-card-body", className)}>{children}</div>;
}

export function Badge({ className, children }: BaseProps) {
  return <span className={join("ui-badge", className)}>{children}</span>;
}

export function Button(
  props: React.ButtonHTMLAttributes<HTMLButtonElement> & {
    variant?: "primary" | "secondary";
  },
) {
  const { className, variant = "primary", ...rest } = props;
  return <button className={join("ui-button", `ui-button-${variant}`, className)} {...rest} />;
}

export function Input(props: React.InputHTMLAttributes<HTMLInputElement>) {
  return <input {...props} className={join("ui-input", props.className)} />;
}

export function ProgressBar({ value }: { value: number }) {
  const safe = Math.max(0, Math.min(100, value));
  return (
    <div className="ui-progress-shell">
      <div className="ui-progress-fill" style={{ width: `${safe}%` }} />
    </div>
  );
}
