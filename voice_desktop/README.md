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
```

Open settings in the app and enter the remote `voice_server` URL, for example `https://api.example.com`. Production deployments should use `https` and `wss`.
