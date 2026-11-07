// @vitest-environment happy-dom
import { createElement } from 'react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { act, cleanup, render, screen, waitFor, within } from '@testing-library/react';
import { afterEach, expect, it } from 'vitest';
import { LagDiagnostics } from './lag-diagnostics';
import { profilesQueryKey } from '../../lib/profiles-api';

afterEach(() => cleanup());
it('starts a new lag history when switching between unnamed and named default profiles', async () => {
  const client = new QueryClient({ defaultOptions: { queries: { staleTime: Infinity, retry: false } } });
  const groupId = 'reader';
  const profiles = { default: {} };
  client.setQueryData(profilesQueryKey, { active: null, profiles });
  const partition = { topic: 'orders', partition: 0, committedOffset: '1', logEndOffset: '5', lag: '4' };
  client.setQueryData(['lag-diagnostics', null, groupId], { partitionLag: [partition] }, { updatedAt: 1000 });
  client.setQueryData(
    ['lag-diagnostics', 'default', groupId],
    { partitionLag: [{ ...partition, lag: '400' }] },
    { updatedAt: 2000 },
  );
  render(createElement(QueryClientProvider, { client }, createElement(LagDiagnostics, { groupId })));
  const history = () =>
    within(screen.getByRole('region', { name: 'Lag sample history', hidden: true })).getAllByRole('row', {
      hidden: true,
    });
  await waitFor(() => expect(history()).toHaveLength(2));
  act(() => {
    client.setQueryData(profilesQueryKey, { active: 'default', profiles });
  });
  await waitFor(() => {
    expect(history()).toHaveLength(2);
    expect(history()[1]?.textContent).toContain('400');
  });
  client.clear();
});
