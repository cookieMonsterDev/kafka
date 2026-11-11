import { expect, test } from '@playwright/test';
import AxeBuilder from '@axe-core/playwright';

const commandPath = 'docs/cli/reference/commands/topic/create/';

test('command navigation expands the active path, filters, and restores the tree', async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto(commandPath);
  const nav = page.locator('aside nav[aria-label="Documentation"]');
  await expect(nav.getByRole('link', { name: 'create', exact: true })).toHaveAttribute('aria-current', 'page');
  await nav.getByRole('searchbox', { name: 'Filter commands' }).fill('offset');
  await expect(nav.getByRole('link', { name: 'reset-offsets', exact: true })).toBeVisible();
  await expect(nav.getByRole('link', { name: 'create', exact: true })).toBeHidden();
  await nav.getByRole('searchbox').fill('topic');
  await expect(nav.getByRole('link', { name: 'create', exact: true })).toBeVisible();
  await nav.getByRole('searchbox').fill('');
  await expect(nav.getByRole('link', { name: 'create', exact: true })).toBeVisible();
  await expect(page.getByRole('navigation', { name: 'Breadcrumb' })).toContainText('topic');
});

test('search fetches once on demand, finds flags, and filters packages', async ({ page }) => {
  let requests = 0;
  page.on('request', (request) => {
    if (request.url().endsWith('/search-index.json')) requests++;
  });
  await page.goto(commandPath);
  expect(requests).toBe(0);
  await page.getByRole('button', { name: 'Search documentation', exact: true }).click();
  const search = page.getByRole('dialog', { name: 'Search documentation' });
  await search.getByRole('combobox', { name: 'Search terms' }).fill('--replication-factor');
  await expect(
    search.getByRole('listbox', { name: 'Search results' }).getByRole('option').filter({ hasText: 'topic create' }),
  ).toBeVisible();
  await expect(search.getByRole('listbox', { name: 'Search results' }).getByRole('option').first()).toContainText(
    'cli',
  );
  await search.getByRole('combobox', { name: 'Scope', exact: true }).click();
  await page.getByRole('option', { name: 'All packages', exact: true }).click();
  await search.getByRole('combobox', { name: 'Search terms' }).fill('discoverConfigFile');
  await expect(search.getByRole('listbox', { name: 'Search results' }).getByRole('option').first()).toContainText(
    'config',
  );
  await page.keyboard.press('Escape');
  await expect(page.getByRole('button', { name: 'Search documentation', exact: true })).toBeFocused();
  await page.getByRole('button', { name: 'Search documentation', exact: true }).click();
  expect(requests).toBe(1);
});

test('search offers recovery after a failed index request', async ({ page }) => {
  await page.route('**/search-index.json', (route) => route.fulfill({ status: 503, body: 'unavailable' }));
  await page.goto(commandPath);
  await page.getByRole('button', { name: 'Search documentation', exact: true }).click();
  await expect(page.getByRole('button', { name: 'Retry search' })).toBeVisible();
  await page.unroute('**/search-index.json');
  await page.getByRole('button', { name: 'Retry search' }).click();
  await expect(page.getByRole('listbox', { name: 'Search results' }).getByRole('option').first()).toBeVisible();
});

test('mobile menu contains focus, closes with Escape, and restores its opener', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto(commandPath);
  const opener = page.getByRole('button', { name: 'Open documentation menu', exact: true });
  await opener.click();
  const menu = page.getByRole('dialog', { name: 'Documentation', exact: true });
  await expect(menu).toBeVisible();
  for (let index = 0; index < 25; index++) {
    await page.keyboard.press('Tab');
    expect(await menu.evaluate((element) => element.contains(document.activeElement))).toBe(true);
  }
  await page.keyboard.press('Escape');
  await expect(menu).not.toBeVisible();
  await expect(opener).toBeFocused();
});

for (const theme of ['dark'] as const) {
  for (const path of ['docs/core/start/getting-started/', commandPath, 'docs/core/reference/configuration/']) {
    test(`accessibility: ${theme} ${path}`, async ({ page }) => {
      await page.addInitScript((value) => localStorage.setItem('docs-theme', value), theme);
      await page.setViewportSize({ width: 1440, height: 900 });
      await page.goto(path);
      await expect(page.locator('meta[name="color-scheme"]')).toHaveAttribute('content', 'dark');
      await expect(page.getByRole('button', { name: /Theme:/ })).toHaveCount(0);
      const result = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag21aa', 'wcag22aa']).analyze();
      expect(result.violations).toEqual([]);
    });
  }
}

test('mobile menu and search pass accessibility checks', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto(commandPath);
  await page.getByRole('button', { name: 'Open documentation menu', exact: true }).click();
  expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
  await page.keyboard.press('Escape');
  await expect(page.getByRole('dialog', { name: 'Documentation', exact: true })).toBeHidden();
  await page.getByRole('button', { name: 'Search documentation', exact: true }).click();
  await expect(page.getByRole('listbox', { name: 'Search results' }).getByRole('option').first()).toBeVisible();
  expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
});

test('narrow layout reflows and newly overflowing code remains keyboard scrollable', async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto('docs/core/start/getting-started/');
  await page.setViewportSize({ width: 320, height: 800 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  await expect
    .poll(async () =>
      page
        .locator('pre.astro-code')
        .evaluateAll((elements) =>
          elements
            .filter((el) => el.scrollWidth > el.clientWidth + 1)
            .every((el) => el.getAttribute('tabindex') === '0'),
        ),
    )
    .toBe(true);
  await page.emulateMedia({ reducedMotion: 'reduce' });
  await page.getByRole('button', { name: 'Open documentation menu', exact: true }).click();
  await expect(page.getByRole('dialog', { name: 'Documentation', exact: true })).toBeVisible();
});

test('static pages expose Markdown, versions and skill links without JavaScript', async ({ browser }) => {
  const context = await browser.newContext({ javaScriptEnabled: false });
  const page = await context.newPage();
  await page.goto('http://127.0.0.1:4329/kafka/' + commandPath);
  await expect(page.getByRole('heading', { name: 'topic create', exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Copy page' })).toBeHidden();
  await expect(
    page.locator('aside nav[aria-label="Packages"]').getByRole('link', { name: 'Studio', exact: true }),
  ).toBeVisible();
  const href = await page.getByRole('link', { name: 'View Markdown' }).getAttribute('href');
  const response = await page.request.get(href!);
  expect(response.ok()).toBe(true);
  expect(await response.text()).toContain('Canonical: https://cookiemonsterdev.github.io/kafka/');
  const manifest = await (await page.request.get('http://127.0.0.1:4329/kafka/docs-index.json')).json();
  expect(manifest.skills.length).toBeGreaterThan(8);
  for (const skill of manifest.skills) {
    const url = new URL(skill.url);
    expect((await page.request.get('http://127.0.0.1:4329' + url.pathname)).ok()).toBe(true);
  }
  await context.close();
});

test('mobile header fills the middle with search and keeps burger/beer and GitHub at the sides', async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto(commandPath);
  const header = page.locator('header');
  const search = header.getByRole('button', { name: 'Search documentation', exact: true });
  expect((await search.boundingBox())!.width).toBeGreaterThan(200);
  await expect(header.getByRole('link', { name: 'Kafka', exact: true })).toBeHidden();
  await expect(header.getByRole('link', { name: 'GitHub repository' })).toBeVisible();
  await page.getByRole('button', { name: 'Open documentation menu' }).click();
  const menu = page.getByRole('dialog', { name: 'Documentation', exact: true });
  await expect(menu.getByRole('button', { name: 'Close documentation menu' })).toBeVisible();
  await menu.getByRole('combobox', { name: 'Package' }).click();
  await expect(menu.getByRole('option', { name: /Studio/ })).toBeVisible();
  await page.keyboard.press('Escape');
  await expect(menu).toBeVisible();
  await menu.getByRole('button', { name: 'Search documentation' }).click();
  await expect(page.getByRole('dialog', { name: 'Search documentation' })).toBeVisible();
  await page.getByRole('combobox', { name: 'Search terms' }).fill('topic');
  await page.keyboard.press('Escape');
  await expect(menu.getByRole('button', { name: 'Search documentation' })).toBeFocused();
  await menu.getByRole('button', { name: 'Close documentation menu' }).click();
  await expect(menu).not.toBeVisible();
});

test('short landscape search keeps its final result inside the dialog', async ({ page }) => {
  await page.setViewportSize({ width: 844, height: 390 });
  await page.goto(commandPath);
  await page.getByRole('button', { name: 'Search documentation', exact: true }).click();
  const dialog = page.getByRole('dialog', { name: 'Search documentation' });
  const results = dialog.getByRole('listbox', { name: 'Search results' });
  await expect(results.getByRole('option').first()).toBeVisible();
  await results.evaluate((list) => {
    list.scrollTop = list.scrollHeight;
  });
  const listBox = (await results.boundingBox())!;
  const bounds = (await dialog.boundingBox())!;
  expect(listBox.y + listBox.height).toBeLessThanOrEqual(bounds.y + bounds.height + 1);
});

test('mobile documentation navigation works without JavaScript', async ({ browser }) => {
  const context = await browser.newContext({ javaScriptEnabled: false, viewport: { width: 390, height: 844 } });
  const page = await context.newPage();
  await page.goto('http://127.0.0.1:4329/kafka/' + commandPath);
  const fallback = page.locator('noscript aside');
  await expect(
    fallback.getByRole('navigation', { name: 'Packages' }).getByRole('link', { name: 'studio', exact: true }),
  ).toBeVisible();
  await expect(fallback.getByRole('link', { name: 'create', exact: true })).toBeVisible();
  await context.close();
});

test('sidebar keeps its scroll position and expanded groups across navigation', async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto(commandPath);
  const sidebar = page.locator('#docs-sidebar-scroll');
  const group = sidebar.locator('details[data-command-group]:not([open])').first();
  const groupLabel = await group.locator('summary').innerText();
  await group.locator('summary').click();
  await sidebar.evaluate((element) => {
    element.scrollTop = element.scrollHeight;
  });
  const scrollTop = await sidebar.evaluate((element) => element.scrollTop);
  await sidebar.getByRole('link', { name: 'Embedding the CLI', exact: true }).click();
  await expect(page.getByRole('heading', { level: 1, name: 'Embedding the CLI' })).toBeVisible();
  expect(await sidebar.evaluate((element) => element.scrollTop)).toBe(scrollTop);
  await expect(sidebar.locator('details[data-command-group]', { hasText: groupLabel }).first()).toHaveAttribute('open');
});

test('JavaScript-only controls do not shift the page while loading', async ({ page }) => {
  await page.addInitScript(() => {
    const shifts: number[] = [];
    Object.assign(window, { layoutShifts: shifts });
    new PerformanceObserver((list) => {
      for (const entry of list.getEntries()) shifts.push((entry as PerformanceEntry & { value: number }).value);
    }).observe({ type: 'layout-shift', buffered: true });
  });
  for (const viewport of [
    { width: 1440, height: 900 },
    { width: 390, height: 844 },
  ]) {
    await page.setViewportSize(viewport);
    await page.goto(commandPath);
    await expect(page.getByRole('button', { name: 'Copy page' })).toBeVisible();
    await page.waitForLoadState('networkidle');
    const total = await page.evaluate(() =>
      (window as unknown as { layoutShifts: number[] }).layoutShifts.reduce((sum, value) => sum + value, 0),
    );
    expect(total).toBeLessThan(0.01);
  }
});
