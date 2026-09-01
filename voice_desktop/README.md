# Voice Desktop

Standalone macOS client built with Electron, React, and TypeScript. It connects to a remotely deployed `voice_server` over HTTP/WebSocket. It does not bundle, start, or manage the backend.

```bash
npm install
npm run dev
# in a second terminal
npm run electron
npm run typecheck
npm test
npm run build
npm run package:mac
# Windows 安装包（需在 Windows 上执行）：npm run package:win
# Linux 安装包（需在 Linux 上执行）：npm run package:linux
```

The macOS Apple Silicon installer is written to `release/` as a `.dmg` file.

Open settings in the app and enter the remote `voice_server` URL, for example `https://api.example.com`. Production deployments should use `https` and `wss`.
