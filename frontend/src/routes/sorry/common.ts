import { allow } from '$lib/captcha/rateLimiter';
import { redirect } from '@sveltejs/kit';
import { verifyDigest } from '$lib/captcha/lib';

export const checkCaptcha = async (
  searchParams: URLSearchParams,
  challenge: number[],
  getClientAddress: () => string,
) => {
  const digest = searchParams.get('digest');

  if (digest) {
    if (await verifyDigest(digest, challenge)) {
      await allow(getClientAddress());
      let redirectTo = '/';
      try {
        const redirectToUrl = new URL(searchParams.get('redirectTo') || '/');
        redirectTo = `${redirectToUrl.pathname}?${redirectToUrl.searchParams}`;
      } catch (err) {
        console.error('sorry/common', err);
      }

      redirect(302, redirectTo);
    }
  }
};
