import { useState } from 'react';
import { Pencil, Save, Shuffle, Trash2 } from 'lucide-react';
import { Button } from '../ui/button';
import { Select, SelectContent, SelectGroup, SelectItem, SelectLabel, SelectTrigger, SelectValue } from '../ui/select';
import {
  deletePayloadTemplate,
  listSavedPayloadTemplates,
  renamePayloadTemplate,
  savePayloadTemplate,
  type SavedPayloadTemplate,
} from '../../lib/payload-templates';
import { PAYLOAD_TEMPLATES } from './templates';
import { TemplateNameDialog } from './template-name-dialog';

export interface PayloadTemplatePickerProps {
  readonly currentKey: string;
  readonly currentValue: string;
  readonly onApply: (key: string, value: string) => void;
}

type NameDialogRequest = { readonly mode: 'save' } | { readonly mode: 'rename'; readonly id: string };

/** The "use a template" select plus its shuffle/save/rename/delete actions, backed by the built-in templates and whatever the viewer has saved locally. */
export function PayloadTemplatePicker({ currentKey, currentValue, onApply }: PayloadTemplatePickerProps) {
  const [templateId, setTemplateId] = useState<string | null>(null);
  const [savedTemplates, setSavedTemplates] = useState<readonly SavedPayloadTemplate[]>(() =>
    listSavedPayloadTemplates(),
  );
  const [nameDialog, setNameDialog] = useState<NameDialogRequest | null>(null);

  const builtInTemplate = PAYLOAD_TEMPLATES.find((entry) => entry.id === templateId);
  const savedTemplate = savedTemplates.find((entry) => entry.id === templateId);

  function applyTemplate(id: string): void {
    const built = PAYLOAD_TEMPLATES.find((entry) => entry.id === id)?.build();
    if (built !== undefined) {
      setTemplateId(id);
      onApply(built.key, built.value);
      return;
    }
    const saved = savedTemplates.find((entry) => entry.id === id);
    if (saved === undefined) return;
    setTemplateId(id);
    onApply(saved.key, saved.value);
  }

  function randomizeTemplate(): void {
    if (builtInTemplate !== undefined) applyTemplate(builtInTemplate.id);
  }

  function saveAsTemplate(label: string): void {
    const saved = savePayloadTemplate({ label, key: currentKey, value: currentValue });
    setSavedTemplates((current) => [...current, saved]);
    setTemplateId(saved.id);
  }

  function removeSavedTemplate(id: string): void {
    deletePayloadTemplate(id);
    setSavedTemplates((current) => current.filter((entry) => entry.id !== id));
    setTemplateId((current) => (current === id ? null : current));
  }

  function renameSavedTemplate(id: string, label: string): void {
    const renamed = renamePayloadTemplate(id, label);
    if (renamed === null) return;
    setSavedTemplates((current) => current.map((entry) => (entry.id === id ? renamed : entry)));
  }

  return (
    <div className="flex items-center gap-2">
      <Select value={templateId ?? undefined} onValueChange={applyTemplate}>
        <SelectTrigger size="sm" className="w-56" aria-label="Payload template">
          <SelectValue placeholder="Use a template…" />
        </SelectTrigger>
        <SelectContent>
          <SelectGroup>
            <SelectLabel>Built-in</SelectLabel>
            {PAYLOAD_TEMPLATES.map((template) => (
              <SelectItem key={template.id} value={template.id}>
                {template.label}
              </SelectItem>
            ))}
          </SelectGroup>
          {savedTemplates.length > 0 && (
            <SelectGroup>
              <SelectLabel>Your templates</SelectLabel>
              {savedTemplates.map((template) => (
                <SelectItem key={template.id} value={template.id}>
                  {template.label}
                </SelectItem>
              ))}
            </SelectGroup>
          )}
        </SelectContent>
      </Select>
      <Button
        type="button"
        variant="outline"
        size="sm"
        disabled={builtInTemplate === undefined}
        onClick={randomizeTemplate}
        aria-label="Regenerate from the selected template"
      >
        <Shuffle className="size-4" aria-hidden="true" />
      </Button>
      {savedTemplate !== undefined && (
        <>
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={() => setNameDialog({ mode: 'rename', id: savedTemplate.id })}
            aria-label="Rename this saved template"
          >
            <Pencil className="size-4" aria-hidden="true" />
          </Button>
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={() => removeSavedTemplate(savedTemplate.id)}
            aria-label="Delete this saved template"
          >
            <Trash2 className="size-4" aria-hidden="true" />
          </Button>
        </>
      )}
      <Button
        type="button"
        variant="outline"
        size="sm"
        onClick={() => setNameDialog({ mode: 'save' })}
        aria-label="Save the current payload as a template"
      >
        <Save className="size-4" aria-hidden="true" />
      </Button>
      {nameDialog?.mode === 'save' && (
        <TemplateNameDialog
          key="save"
          onOpenChange={(open) => {
            if (!open) setNameDialog(null);
          }}
          title="Save as template"
          description="Stores the current key and value in this browser for reuse later."
          confirmLabel="Save"
          onSubmit={saveAsTemplate}
        />
      )}
      {nameDialog?.mode === 'rename' && (
        <TemplateNameDialog
          key={`rename-${nameDialog.id}`}
          onOpenChange={(open) => {
            if (!open) setNameDialog(null);
          }}
          title="Rename template"
          description="Renames this saved template."
          confirmLabel="Rename"
          initialLabel={savedTemplates.find((entry) => entry.id === nameDialog.id)?.label}
          onSubmit={(label) => renameSavedTemplate(nameDialog.id, label)}
        />
      )}
    </div>
  );
}
