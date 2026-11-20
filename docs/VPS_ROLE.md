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

The default media mode is `p2p`:

- remote listeners load the control page through the VPS relay
- WebSocket signaling also uses the VPS relay
- audio comes only from the Go server RTP seed or peer relays
  over WebRTC DataChannels
- the relay rejects `/native/`, `/source/` and `/media/`

A direct station endpoint is not required for the p2p mode. If you choose
`RADIO_MEDIA_MODE=direct`, then the station needs its own direct HTTPS endpoint
and `RADIO_DIRECTORY_PUBLIC_URL` must be that endpoint.

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

## Implemented posture for this project

1. Relay defaults `RELAY_ALLOW_MEDIA=0`; media paths return 403.
2. Station defaults `RADIO_MEDIA_MODE=p2p`; no HTTP/native fallback from a
   remote browser.
3. Only a loopback browser gets the seed role. A remote browser reached
   through the relay cannot become a seed and cannot pull `/source` through
   the VPS.
4. The host seed reads `/source` over loopback and forwards frames to remote
   listeners over WebRTC; remote children can relay to their children.
5. If the seed is offline, remote listeners wait/retry. This is the deliberate
   tradeoff for keeping the VPS out of media.
