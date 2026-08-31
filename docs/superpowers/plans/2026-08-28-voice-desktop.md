# Voice Desktop Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a standalone Electron + React + TypeScript macOS client that connects to a remote `voice_server` over HTTP/WebSocket.

**Architecture:** Electron owns the native window and secure preload boundary. React owns the full-flow conversation screen and browser audio APIs. Small services isolate endpoint settings, voice protocol, recording, and playback so the UI remains easy to read.

**Tech Stack:** Electron, React 18, TypeScript, Vite, Vitest, native WebSocket/Web Audio APIs.

**Spec:** `docs/superpowers/specs/2026-08-28-voice-desktop-design.md`

## Global Constraints

- The desktop client must not bundle or start `voice_server`.
- The client communicates with remote services using HTTP/WebSocket only.
- Electron renderer uses `contextIsolation: true` and `nodeIntegration: false`.
- Keep the first release focused on the full-flow conversation screen.

### Task 1: Scaffold Electron and React

**Files:** create `voice_desktop/package.json`, `tsconfig.json`, `vite.config.ts`, `index.html`, `electron/main.ts`, `electron/preload.ts`, `src/main.tsx`, `src/App.tsx`.

- [ ] Add scripts for `dev`, `build`, `test`, and `typecheck`.
- [ ] Create a BrowserWindow with the secure web preferences and Vite/dev-or-dist loading.
- [ ] Add a minimal React root that renders without backend access.
- [ ] Run `npm install` and `npm run typecheck`.

### Task 2: Add protocol and connection services

**Files:** create `src/types/voice-protocol.ts`, `src/services/voice-server-client.ts`, `src/services/settings-service.ts`, `tests/voice-server-client.test.ts`.

- [ ] Normalize base URLs and build HTTP/WS endpoints.
- [ ] Implement a typed WebSocket client with start, binary audio, interrupt, stop, and event callbacks.
- [ ] Add tests for URL normalization and session URL generation.
- [ ] Run the focused Vitest test.

### Task 3: Build audio and conversation state

**Files:** create `src/services/audio-recorder.ts`, `src/services/audio-player.ts`, `src/features/conversation/conversation-types.ts`, `src/features/conversation/conversation-store.ts`, `tests/conversation-store.test.ts`.

- [ ] Implement microphone capture with 16 kHz mono PCM conversion and clean stop.
- [ ] Implement queued WAV playback with interrupt-safe cleanup.
- [ ] Add reducer-style conversation state transitions for user partial/final, assistant delta, TTS, and errors.
- [ ] Test state transitions.

### Task 4: Implement the desktop conversation UI

**Files:** create `src/features/conversation/ConversationPage.tsx`, `src/components/AppShell.tsx`, `src/components/ConnectionStatus.tsx`, `src/components/MessageBubble.tsx`, `src/components/VoiceControls.tsx`, `src/components/SettingsPanel.tsx`, `src/styles/globals.css`.

- [ ] Build a compact macOS-oriented layout with settings, status, transcript, and controls.
- [ ] Wire start/interrupt/stop to the services and render all state transitions.
- [ ] Add clear loading, empty, disconnected, and error states.
- [ ] Run production build and typecheck.

### Task 5: Document usage and verify packaging shape

**Files:** create `voice_desktop/README.md`, `voice_desktop/.gitignore`.

- [ ] Document remote backend configuration and commands.
- [ ] Confirm the project is outside Cargo workspace and contains no server startup code.
- [ ] Run `npm test`, `npm run typecheck`, and `npm run build` from `voice_desktop`.
