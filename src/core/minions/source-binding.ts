/**
 * Job source bindings are an execution scope, unlike arbitrary nested payload
 * content. `__all__` is a read-only federation selector and cannot be used by
 * a source-bound background write.
 */
export type JobSourceBindingField = 'sourceId' | 'source_id';

export function allSourcesJobBindingField(
  data: Record<string, unknown> | undefined,
): JobSourceBindingField | null {
  if (!data || typeof data !== 'object') return null;
  if (data.sourceId === '__all__') return 'sourceId';
  if (data.source_id === '__all__') return 'source_id';
  return null;
}

export function allSourcesJobBindingMessage(field: JobSourceBindingField): string {
  return (
    `job payload data.${field} cannot be "__all__": "__all__" is a read-only federation selector; ` +
    'jobs that bind a source must use one concrete source ID.'
  );
}

export function assertNoAllSourcesJobBinding(data: Record<string, unknown> | undefined): void {
  const field = allSourcesJobBindingField(data);
  if (field) throw new Error(allSourcesJobBindingMessage(field));
}
