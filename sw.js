// ══════════════════════════════════════════════════════════
// Service Worker — Slovenska košarka (Liga OTP banka)
// Strategija: NETWORK-FIRST (vedno sveže). Predpomnilnik je le
// zasilni padalo, če mreže res ni. Hard refresh NI več potreben.
// ══════════════════════════════════════════════════════════

const CACHE = 'skl-net-first-v1';

// Ob namestitvi: takoj prevzemi krmilo (ne čakaj na zaprtje zavihkov)
self.addEventListener('install', (e) => {
  self.skipWaiting();
});

// Ob aktivaciji: počisti VSE stare predpomnilnike in prevzemi vse odprte zavihke
self.addEventListener('activate', (e) => {
  e.waitUntil(
    (async () => {
      const keys = await caches.keys();
      await Promise.all(keys.map((k) => (k === CACHE ? null : caches.delete(k))));
      await self.clients.claim();
    })()
  );
});

// Vsaka zahteva gre NAJPREJ na mrežo. Predpomnilnik samo, če mreže ni.
self.addEventListener('fetch', (e) => {
  const req = e.request;

  // Samo GET zahteve obravnavamo
  if (req.method !== 'GET') return;

  e.respondWith(
    (async () => {
      try {
        // VEDNO poskusi svežo verzijo z mreže
        const fresh = await fetch(req, { cache: 'no-store' });
        // Shrani kopijo v predpomnilnik (samo za zasilni offline primer)
        if (fresh && fresh.status === 200 && req.url.startsWith('http')) {
          const cache = await caches.open(CACHE);
          cache.put(req, fresh.clone());
        }
        return fresh;
      } catch (err) {
        // Mreže ni → vrni zadnjo znano kopijo, če obstaja
        const cached = await caches.match(req);
        if (cached) return cached;
        throw err;
      }
    })()
  );
});

// Dovoli strani, da ročno sproži takojšnjo posodobitev
self.addEventListener('message', (e) => {
  if (e.data === 'SKIP_WAITING') self.skipWaiting();
});
