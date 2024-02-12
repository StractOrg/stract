export const globals = async (
  globals?: App.PageData['globals'],
): Promise<App.PageData['globals']> => ({
  ...globals,
});
