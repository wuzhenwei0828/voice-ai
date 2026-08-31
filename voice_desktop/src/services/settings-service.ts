const KEY = 'voice-desktop-settings';
export type Settings = { baseUrl: string; token: string; voice?: string };

export function loadSettings(): Settings {
  try {
    const value = JSON.parse(localStorage.getItem(KEY) ?? '{}');
    return { baseUrl: value.baseUrl ?? 'http://127.0.0.1:8080', token: value.token ?? '', voice: value.voice ?? '' };
  } catch { return { baseUrl: 'http://127.0.0.1:8080', token: '', voice: '' }; }
}

export function saveSettings(settings: Settings) { localStorage.setItem(KEY, JSON.stringify(settings)); }
