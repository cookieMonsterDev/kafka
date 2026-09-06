const STORAGE_KEY = 'kafka-studio-payload-templates:v1';

export interface SavedPayloadTemplate {
  readonly id: string;
  readonly label: string;
  readonly key: string;
  readonly value: string;
  readonly createdAt: string;
}

function isSavedPayloadTemplate(candidate: unknown): candidate is SavedPayloadTemplate {
  if (typeof candidate !== 'object' || candidate === null) return false;
  const { id, label, key, value, createdAt } = candidate as Record<string, unknown>;
  return (
    typeof id === 'string' &&
    typeof label === 'string' &&
    typeof key === 'string' &&
    typeof value === 'string' &&
    typeof createdAt === 'string'
  );
}

/** Reads the saved template list, discarding anything corrupted rather than failing the whole read. */
export function listSavedPayloadTemplates(): readonly SavedPayloadTemplate[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw === null) return [];
    const parsed: unknown = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed.filter(isSavedPayloadTemplate);
  } catch {
    return [];
  }
}

function persist(templates: readonly SavedPayloadTemplate[]): void {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(templates));
  } catch {
    // private mode, quota, or disabled storage — the save silently doesn't stick
  }
}

/** Appends a new template, generating its id and timestamp, and persists the full list. */
export function savePayloadTemplate(input: {
  readonly label: string;
  readonly key: string;
  readonly value: string;
}): SavedPayloadTemplate {
  const template: SavedPayloadTemplate = { ...input, id: crypto.randomUUID(), createdAt: new Date().toISOString() };
  persist([...listSavedPayloadTemplates(), template]);
  return template;
}

export function deletePayloadTemplate(id: string): void {
  persist(listSavedPayloadTemplates().filter((template) => template.id !== id));
}

/** Returns the updated template, or `null` if `id` doesn't match anything saved. */
export function renamePayloadTemplate(id: string, label: string): SavedPayloadTemplate | null {
  const templates = listSavedPayloadTemplates();
  const renamed = templates.map((template) => (template.id === id ? { ...template, label } : template));
  persist(renamed);
  return renamed.find((template) => template.id === id) ?? null;
}
