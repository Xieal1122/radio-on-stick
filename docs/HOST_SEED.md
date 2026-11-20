# Host seed and P2P-only media

With `RADIO_MEDIA_MODE=p2p`, remote browsers never fetch `/source` or
`/native` through the VPS relay. A browser connected directly to the station
host is the initial media seed.

## Manual host page

On the station host, open:

```text
http://127.0.0.1:8091/?seed=1
```

If the local owner redirects to `/admin`, use:

```text
http://127.0.0.1:8091/admin?seed=1
```

The loopback page automatically requests the `seed` role and starts the mesh
without a user gesture when `seed=1` is present.

## Headless seed

On macOS the included script starts a dedicated headless Chrome seed:

```bash
RADIO_GO_ADDR=127.0.0.1:8091 bash deploy/host-seed.sh
```

Stop it with:

```bash
kill "$(cat data/campus/host-seed.pid)"
```

The seed process only reads `/source` over loopback. Remote listeners receive
audio from the seed or from other peer relays over WebRTC DataChannels.

## If no seed is running

Remote listeners stay in "P2P 连接中" / buffering. They will not fall back to
the VPS for media. Keep one station-host browser or the headless seed online.
