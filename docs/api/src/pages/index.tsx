import { Redirect } from '@docusaurus/router';


function NoscriptRedirect() {
  return (
    <noscript>
      <meta http-equiv="refresh" content="0;url=https://stract.com/beta/api/docs/swagger" />
    </noscript>
  );
}

export default function Home(): JSX.Element {
  return (
    <>
      <NoscriptRedirect />
      <Redirect to="/api" />
    </>
  );
}
