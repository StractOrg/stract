import { exec } from "child_process";
import * as path from "path";
import { workspace, ExtensionContext, window } from "vscode";

import {
    Executable,
    LanguageClient,
    LanguageClientOptions,
    ServerOptions,
} from "vscode-languageclient/node";

let client: LanguageClient;

export async function activate(context: ExtensionContext) {
    const traceOutputChannel = window.createOutputChannel(
        "Optic Language Server trace"
    );
    const stdout: string = await new Promise((res) =>
        exec(
            "cargo metadata --no-deps",
            { cwd: context.extensionPath },
            (_err, stdout) => res(stdout)
        )
    );
    const targetDir: string = JSON.parse(stdout).target_directory;

    const serverModule = path.join(targetDir, "debug", "optic-lsp");
    // The debug options for the server
    // --inspect=6009: runs the server in Node's Inspector mode so VS Code can attach to the server for debugging
    const debugOptions = { execArgv: ["--nolazy", "--inspect=6009"] };

    const run: Executable = {
        command: serverModule,
        options: {},
    };

    // If the extension is launched in debug mode then the debug server options are used
    // Otherwise the run options are used
    const serverOptions: ServerOptions = {
        run,
        debug: run,
    };

    // Options to control the language client
    const clientOptions: LanguageClientOptions = {
        // Register the server for plain text documents
        documentSelector: [{ scheme: "file", language: "optic" }],
        synchronize: {
            // Notify the server about file changes to '.clientrc files contained in the workspace
            fileEvents: workspace.createFileSystemWatcher("**/.clientrc"),
        },
        traceOutputChannel,
    };

    // Create the language client and start the client.
    client = new LanguageClient(
        "opticLspServer",
        "Optic LSP Server",
        serverOptions,
        clientOptions
    );

    // Start the client. This will also launch the server
    client.start();
}

export function deactivate(): Thenable<void> | undefined {
    if (!client) {
        return undefined;
    }
    return client.stop();
}