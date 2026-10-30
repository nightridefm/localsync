# TLS gateway

nginx in front of the engine, configured like the apps' production
gateways: TLS with a real certificate, HTTP/2 and HTTP/3, gzip on, and
nginx's default proxy timeouts (60 s) and buffering. It lets the harness,
the simulation and the browser tests run over `wss://` and `https://`
the way browsers reach the apps (design section 14.3).

- `8443`: the harness engine (`127.0.0.1:3141`).
- `8444`: the browser tests' engine (`127.0.0.1:3142`) and their pages
  (`127.0.0.1:8078`) on one origin.
- `/hostile/` on both: a middlebox that refuses WebSocket upgrades and
  holds every response until it is complete
  (`ts/harness/src/middlebox.ts` on `8079`, started by the tests). It
  exercises the client's fallback to HTTP and to long polls.

## Certificate

The gateway needs a certificate for a hostname that resolves to
127.0.0.1, read from an existing Let's Encrypt (certbot) Docker volume.
Name both in `.env` at the repo root (gitignored):

```sh
LOCALSYNC_TLS_VOLUME=<docker volume holding /etc/letsencrypt>
LOCALSYNC_TLS_DOMAIN=<hostname with a certificate in it, resolving to 127.0.0.1>
```

## Run

```sh
docker compose --profile gateway up -d gateway
pnpm --filter @nightride/localsync-harness gateway            # wss, https, auto, hostile
LOCALSYNC_URL=wss://$LOCALSYNC_TLS_DOMAIN:8443/sync \
  pnpm --filter @nightride/localsync-harness matrix --adapter=localsync
LOCALSYNC_BROWSER_VIA=gateway pnpm --filter @nightride/localsync-browser smoke
LOCALSYNC_BROWSER_VIA=hostile LOCALSYNC_TRANSPORT=auto \
  pnpm --filter @nightride/localsync-browser smoke
```

`LOCALSYNC_TRANSPORT` is `websocket` (the harness default), `http` or
`auto`. `LOCALSYNC_HTTP_URL` overrides the `/sync/http` URL; adding
`?mode=poll` to it forces long polls.
