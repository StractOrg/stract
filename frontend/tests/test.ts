import { expect, test } from '@playwright/test';

test('index page has a searchbar', async ({ page }) => {
  await page.goto('/');
  await expect(page.getByRole('searchbox')).toBeVisible();
});
