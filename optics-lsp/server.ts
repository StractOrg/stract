import {
    createConnection,
    ProposedFeatures,
    PublishDiagnosticsParams,
    TextDocumentSyncKind,
} from 'vscode-languageserver/node';
import { OpticsBackend } from './optics_lsp';

// Create LSP connection
const connection = createConnection(ProposedFeatures.all);

const sendDiagnosticsCallback = (params: PublishDiagnosticsParams) =>
    connection.sendDiagnostics(params);
const ls = new OpticsBackend(sendDiagnosticsCallback);

connection.onNotification((...args) => ls.onNotification(...args));
connection.onHover((params) => ls.onHover(params));

connection.onInitialize(() => {
    return {
        capabilities: {
            textDocumentSync: {
                openClose: true,
                save: true,
                change: TextDocumentSyncKind.Full,
            },
            hoverProvider: true,
        },
    };
});

connection.listen();