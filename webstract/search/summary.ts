import { signal } from "@preact/signals";

export const summarySignals = signal<
  Record<string, { inProgress: boolean; data: string } | void>
>({});
