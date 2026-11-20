# VPS role: rendezvous, not media

## Intended policy

```text
VPS:
  Directory (invite resolve)
  control/relay rendezvous
  optional relay agent endpoints

NOT VPS:
  /native audio
  /source chunks
  /media uploads
  audio WebSocket data channels / P2P payload
```

The station must expose its own direct HTTPS endpoint (public IP, Cloudflare
Tunnel, or a campus/LAN endpoint) and register that URL as
`RADIO_DIRECTORY_PUBLIC_URL`. Listeners connect directly to the station for the
web app and media; P2P signaling may use the VPS.

## Audit of the old deployment

The old `station.143-20-156-7.sslip.io` Caddy site reverse-proxied the relay
server at `127.0.0.1:18081`. That relay uses yamux and forwards **all HTTP
requests**, including:

```text
/native/<track_id>
/source/manifest
/source/chunk
/media/...
/ws
```

Therefore the old public station domain did carry audio through the VPS when
clients used that domain or its HTTP leaf fallback. P2P DataChannels may bypass
it, but HTTP fallback/`/native` does not. It is not honest to call that
deployment rendezvous-only.

## Making it true

1. Give the station a direct HTTPS endpoint.
2. Register the direct endpoint in the Directory.
3. Do not put the station's public domain behind the relay.
4. Keep relay for control/join only; the standalone relay defaults
   `RELAY_ALLOW_MEDIA=0` and rejects media paths.
5. For P2P, keep `/ws` signaling through the station direct endpoint; use
   STUN/TURN separately if needed. Do not use the VPS as an HTTP audio leaf.

The direct endpoint is a deployment requirement. If no direct endpoint exists,
the VPS is the only reachable path and media will necessarily traverse it.
