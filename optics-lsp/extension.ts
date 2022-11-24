import * as path from "path";
import { ExtensionContext, window, WorkspaceFolder } from "vscode";

import {
    ServerOptions,
    LanguageClient,
    LanguageClientOptions,
    TransportKind,
} from "vscode-languageclient/node";

// One client per workspace folder.
let client: LanguageClient;

export function activate(context: ExtensionContext) {
    const server = context.asAbsolutePath(path.join('out', 'server.js'));
    console.log(server);
    const debugOpts = {
        execArgv: ['--nolazy', `--inspect=6009`],
    };

    // If the extension is launched in debug mode then the debug server options are used
    // Otherwise the run options are used
    const serverOpts: ServerOptions = {
        run: { module: server, transport: TransportKind.ipc },
        debug: {
            module: server,
            transport: TransportKind.ipc,
            options: debugOpts
        }
    };

    // Options to control the language client
    const clientOpts: LanguageClientOptions = {
        // Register the server for plain text documents
        documentSelector: [{ scheme: "file", language: "optic" }],
    };

    // Create the language client and start the client.
    client = new LanguageClient(
        "opticLspServer",
        "Optic LSP Server",
        serverOpts,
        clientOpts
    );
    client.start();
}

export function deactivate(): Thenable<void> | undefined {
    if (!client) {
        return undefined;
    }
    return client.stop();
}