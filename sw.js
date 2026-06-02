// Service Worker — SKL Statistika PWA
// Strategija: app shell (index.html) cache-first za hiter zagon + offline,
// podatki (data/*.json) network-first z cache fallback (sveži, a delujejo offline).

const CACHE = 'skl-stats-v1';
const APP_SHELL = [
  './',
  './index.html',
  './manifest.json',
  './icon-192.png',
  './icon-512.png'
];

// Namestitev — predpomni app shell
self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE).then(c => c.addAll(APP_SHELL).catch(() => {})).then(() => self.skipWaiting())
  );
});

// Aktivacija — počisti stare cache
self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', e => {
  const url = new URL(e.request.url);

  // Samo GET
  if (e.request.method !== 'GET') return;

  // Podatki (GitHub JSON) — network-first, fallback na cache
  if (url.pathname.endsWith('.json') || url.hostname.includes('githubusercontent')) {
    e.respondWith(
      fetch(e.request).then(resp => {
        const copy = resp.clone();
        caches.open(CACHE).then(c => c.put(e.request, copy));
        return resp;
      }).catch(() => caches.match(e.request))
    );
    return;
  }

  // KZS slike in proxyji — cache-first (slike se ne spreminjajo pogosto)
  if (url.hostname.includes('kzs.si') || url.hostname.includes('wsrv.nl') ||
      url.hostname.includes('weserv.nl') || url.pathname.includes('/images/')) {
    e.respondWith(
      caches.match(e.request).then(cached =>
        cached || fetch(e.request).then(resp => {
          const copy = resp.clone();
          caches.open(CACHE).then(c => c.put(e.request, copy));
          return resp;
        }).catch(() => cached)
      )
    );
    return;
  }

  // App shell in ostalo — cache-first, fallback na mrežo
  e.respondWith(
    caches.match(e.request).then(cached =>
      cached || fetch(e.request).then(resp => {
        // Predpomni iste-domene odgovore
        if (url.origin === location.origin) {
          const copy = resp.clone();
          caches.open(CACHE).then(c => c.put(e.request, copy));
        }
        return resp;
      }).catch(() => caches.match('./index.html'))
    )
  );
});
