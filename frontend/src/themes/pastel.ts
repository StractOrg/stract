import { base, theme } from './base';

export const pastelTheme = theme({
  name: 'pastel',
  scheme: 'light',
  colors: {
    primary: base.island[400],
    primaryFocus: base.island[500],
    primaryContent: 'rgb(255, 255, 255)',

    secondary: base.navy[400],
    secondaryFocus: base.navy[500],
    secondaryContent: 'rgb(255, 255, 255)',

    accent: base.cocoa[400],
    accentFocus: base.cocoa[500],
    accentContent: 'rgb(255, 255, 255)',

    neutral: base.slate[600],
    neutralFocus: base.slate[700],
    neutralContent: base.island[100],

    base100: 'rgb(255, 255, 255)',
    base200: base.slate[100],
    base300: base.slate[200],
    baseContent: base.slate[900],

    info: base.navy[300],
    infoContent: base.navy[800],

    success: base.green[300],
    successContent: base.green[800],

    warning: base.gold[200],
    warningContent: base.gold[900],

    error: base.cerise2[300],
    errorContent: base.cerise2[800],

    link: base.blue[700],
    linkVisited: base.indigo[600],
  },
});
