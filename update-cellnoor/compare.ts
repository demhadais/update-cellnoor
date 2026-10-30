const ISO_TIMESTAMP =
  /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$/;

export function equivalent(a: unknown, b: unknown): boolean {
  if (a === null || a === undefined || b === null || b === undefined) {
    return (a ?? null) === (b ?? null);
  }

  if (
    typeof a === "string" &&
    typeof b === "string" &&
    ISO_TIMESTAMP.test(a) &&
    ISO_TIMESTAMP.test(b)
  ) {
    return Date.parse(a) === Date.parse(b);
  }

  // The API stores floats with 32-bit precision
  if (typeof a === "number" && typeof b === "number") {
    return Math.fround(a) === Math.fround(b);
  }

  if (Array.isArray(a) && Array.isArray(b)) {
    return a.length === b.length && a.every((x, i) => equivalent(x, b[i]));
  }

  if (isPlainObject(a) && isPlainObject(b)) {
    const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
    return [...keys].every((k) => equivalent(a[k], b[k]));
  }

  return a === b;
}

export function matches(desired: object, actual: object): boolean {
  const actualFields = actual as Record<string, unknown>;
  return Object.entries(desired).every(([key, value]) =>
    equivalent(value, actualFields[key]),
  );
}

export function diff<Desired, Actual>(
  desired: Desired[],
  actual: Actual[],
  same: (desired: Desired, actual: Actual) => boolean,
): { missing: Desired[]; extra: Actual[] } {
  const extra = [...actual];

  const missing = desired.filter((d) => {
    const i = extra.findIndex((a) => same(d, a));
    if (i === -1) {
      return true;
    }
    extra.splice(i, 1);
    return false;
  });

  return { missing, extra };
}

export function unique<T>(items: T[]): T[] {
  return [...new Map(items.map((i) => [JSON.stringify(i), i])).values()];
}

function isPlainObject(x: unknown): x is Record<string, unknown> {
  return typeof x === "object" && x !== null && !Array.isArray(x);
}
