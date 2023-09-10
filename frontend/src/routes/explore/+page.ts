import type { PageLoad } from './$types';

export const load: PageLoad = () => {
  return {
    globals: {
      header: {
        divider: true,
      },
    },
  };
};
