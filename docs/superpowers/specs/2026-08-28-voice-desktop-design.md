# Voice Desktop Design

## Goal

Create a standalone macOS desktop client in `voice_desktop/` using Electron, React, and TypeScript. The client connects to a remotely deployed `voice_server` over HTTP/WebSocket and never starts, embeds, or manages a backend process.

## Architecture

Electron main process owns the native window and a small preload bridge. The React renderer owns the conversation UI, microphone capture, WebSocket protocol handling, and TTS playback. The backend URL is user-configurable; the renderer derives HTTP and WS endpoints from it and keeps all network traffic remote.

The first slice implements the full-flow conversation experience: connect, start/stop, interrupt, live transcript bubbles, connection status, microphone permission, and a local TTS queue. Development can use Vite; packaged builds load the compiled renderer.

## Security and boundaries

- No local server, child process, shell command, or Rust dependency.
- Electron renderer runs with `contextIsolation` enabled and `nodeIntegration` disabled.
- Only the minimum preload API is exposed.
- Default endpoint validation accepts `http`, `https`, `ws`, and `wss`; production deployments should use `https`/`wss`.
- Credentials are represented by a settings field in the initial slice; persistence is local-only and isolated behind a service so secure storage can be added without changing UI code.

## Protocol

The client uses the existing `/ws/voice/web/{session_id}` route and sends JSON control messages plus binary PCM frames. Incoming messages are normalized into ASR partial/final, LLM delta, TTS audio, and error events. The adapter is intentionally isolated so protocol changes do not spread through React components.

## Verification

The project must install with npm, pass TypeScript/build checks, and expose a Vite development screen. Unit tests cover endpoint normalization and conversation state transitions.
