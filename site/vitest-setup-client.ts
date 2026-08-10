// Runs before each client (browser) test file. `vitest-browser-svelte` +
// @vitest/browser provide `render`, locators, and `expect.element` matchers;
// no jest-dom import is needed in browser mode.
//
// Components are rendered in isolation here (no +layout.svelte wrapper), so
// the app's global stylesheet — UnoCSS utilities (border-*, bg-*, etc.) plus
// the hand-written rules in app.css — never loads unless we pull it in
// ourselves. Without this, computed-style assertions (e.g. a card's border)
// see UA defaults (`border-style: none`) instead of the real utility CSS.
import 'virtual:uno.css';
import './src/app.css';
