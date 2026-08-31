# Desktop PC Fidelity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore the desktop conversation page to the structure, copy, controls, and visual states of `crates/voice_server/static` while preserving streaming audio behavior.

**Architecture:** Keep the Electron/React shell and existing WebSocket/audio services. Rebuild only the conversation surface around the PC page's phone-call layout, derive labels and control availability from the conversation reducer, and render transcript/source/settings sections with React state.

**Tech Stack:** React, TypeScript, Vite, Electron, CSS, Vitest.

**Spec:** `crates/voice_server/static/index.html`, `crates/voice_server/static/style.css`, and `crates/voice_server/static/app.js`.

## Global Constraints

- Preserve PCM streaming playback and stale-request filtering.
- A server/TTS error must leave the microphone session state visible and expose retry without enabling a second start action.
- Use the PC Chinese labels and phase mapping exactly where the desktop surface has an equivalent.
- Keep responsive behavior usable at desktop and narrow Electron windows.

### Task 1: Lock state behavior with tests

**Files:**
- Modify: `voice_desktop/tests/conversation-store.test.ts`
- Modify: `voice_desktop/src/features/conversation/conversation-store.ts`

- [x] Add tests for PC phase labels/mapping, server error retry state, and closed-state cleanup.
- [x] Run the focused tests and confirm the new assertions fail before implementation changes.
- [x] Implement the smallest reducer changes needed for those assertions.
- [x] Run the focused tests again and confirm they pass.

### Task 2: Rebuild the React conversation surface

**Files:**
- Modify: `voice_desktop/src/features/conversation/ConversationPage.tsx`
- Modify: `voice_desktop/src/components/VoiceControls.tsx`
- Modify: `voice_desktop/src/components/SettingsPanel.tsx`
- Modify: `voice_desktop/src/services/settings-service.ts`

- [x] Render the PC header status badges, phone stage/avatar waves, mute controls, circular call buttons, collapsible transcript/source/settings areas, and exact phase copy.
- [x] Preserve existing start/interrupt/stop/retry and streaming TTS callbacks.
- [x] Add persisted TTS voice selection and pass it in `session_start`.
- [x] Typecheck the desktop app.

### Task 3: Match the PC visual system

**Files:**
- Modify: `voice_desktop/src/styles/globals.css`

- [x] Port the PC phone-call layout and state-dependent wave/avatar/button/details styles into the desktop stylesheet.
- [x] Add responsive rules matching the PC narrow-window breakpoints.
- [x] Build the desktop app and inspect the generated output for CSS/TypeScript errors.

### Task 4: Regression verification

- [x] Run `npm test` in `voice_desktop`.
- [x] Run `npm run typecheck` in `voice_desktop`.
- [x] Run `npm run build` in `voice_desktop`.
- [x] Review the diff for unrelated changes and report any environment-only Electron install caveat separately.
