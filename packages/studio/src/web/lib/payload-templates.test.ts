import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  deletePayloadTemplate,
  listSavedPayloadTemplates,
  renamePayloadTemplate,
  savePayloadTemplate,
} from './payload-templates';

function fakeLocalStorage(): Storage {
  const store = new Map<string, string>();
  return {
    getItem: (key) => store.get(key) ?? null,
    setItem: (key, value) => store.set(key, value),
    removeItem: (key) => store.delete(key),
    clear: () => store.clear(),
    key: (index) => Array.from(store.keys())[index] ?? null,
    get length() {
      return store.size;
    },
  };
}

beforeEach(() => {
  vi.stubGlobal('localStorage', fakeLocalStorage());
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('listSavedPayloadTemplates', () => {
  it('returns an empty list when nothing is stored', () => {
    expect(listSavedPayloadTemplates()).toEqual([]);
  });

  it('discards corrupted storage instead of throwing', () => {
    localStorage.setItem('kafka-studio-payload-templates:v1', 'not json');
    expect(listSavedPayloadTemplates()).toEqual([]);
  });

  it('filters out malformed entries but keeps valid ones', () => {
    localStorage.setItem(
      'kafka-studio-payload-templates:v1',
      JSON.stringify([{ id: '1', label: 'ok', key: '', value: '{}', createdAt: 'now' }, { id: '2' }]),
    );
    expect(listSavedPayloadTemplates()).toEqual([{ id: '1', label: 'ok', key: '', value: '{}', createdAt: 'now' }]);
  });
});

describe('savePayloadTemplate', () => {
  it('persists a template with a generated id and timestamp', () => {
    const saved = savePayloadTemplate({ label: 'My template', key: 'k', value: '{"a":1}' });
    expect(saved.label).toBe('My template');
    expect(saved.id).toEqual(expect.any(String));
    expect(listSavedPayloadTemplates()).toEqual([saved]);
  });

  it('appends to existing templates rather than replacing them', () => {
    savePayloadTemplate({ label: 'First', key: '', value: '{}' });
    savePayloadTemplate({ label: 'Second', key: '', value: '{}' });
    expect(listSavedPayloadTemplates().map((template) => template.label)).toEqual(['First', 'Second']);
  });
});

describe('deletePayloadTemplate', () => {
  it('removes only the matching template', () => {
    const first = savePayloadTemplate({ label: 'First', key: '', value: '{}' });
    const second = savePayloadTemplate({ label: 'Second', key: '', value: '{}' });
    deletePayloadTemplate(first.id);
    expect(listSavedPayloadTemplates()).toEqual([second]);
  });

  it('is a no-op when the id does not match anything', () => {
    savePayloadTemplate({ label: 'First', key: '', value: '{}' });
    deletePayloadTemplate('does-not-exist');
    expect(listSavedPayloadTemplates()).toHaveLength(1);
  });
});

describe('renamePayloadTemplate', () => {
  it('updates only the matching template and returns it', () => {
    const first = savePayloadTemplate({ label: 'First', key: '', value: '{}' });
    savePayloadTemplate({ label: 'Second', key: '', value: '{}' });
    const renamed = renamePayloadTemplate(first.id, 'Renamed');
    expect(renamed).toEqual({ ...first, label: 'Renamed' });
    expect(listSavedPayloadTemplates().map((template) => template.label)).toEqual(['Renamed', 'Second']);
  });

  it('returns null when the id does not match anything', () => {
    expect(renamePayloadTemplate('does-not-exist', 'New name')).toBeNull();
  });
});
