import { Head } from "$fresh/runtime.ts";

export default function Error404() {
  return (
    <>
      <Head>
        <title>404 - Page not found</title>
      </Head>
      <div class="from-white via-brand/10 to-brand/40 via-30% by bg-gradient-to-b h-full">
        <div class="px-4 py-8 mx-auto">
          <div class="max-w-screen-md mx-auto flex flex-col items-center justify-center">
            <img
              class="my-12"
              src="/images/biglogo.svg"
              alt="the Fresh logo: a sliced lemon dripping with juice"
            />
            <h1 class="text-4xl font-thin">404 - Page not found</h1>
            <p class="my-4">
              The page you were looking for doesn't exist.
            </p>
            <a href="/" class="underline">Go back home</a>
          </div>
        </div>
      </div>
    </>
  );
}
