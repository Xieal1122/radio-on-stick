# radio-go

Go-first Radio on Stick: one codebase for the station, directory, relay server
and relay agent.

This project is intentionally independent of the original Python/PWA checkout.
It contains only the Go services and the browser assets they serve.

## Layout

```text
cmd/station/       host station: playback, library, admin, web UI, /source, /native
cmd/directory/     public invite-code directory
cmd/relay/         rendezvous/control relay server
cmd/relay-agent/   station-side outbound connector to relay
internal/          station, library, stream, mesh, directory, plugin client
web/static/        station web app (PWA)
web/directory/     invite-code entry page
plugins/commercial optional Python third-party source helper (not committed)
deploy/            packages, systemd and Caddy examples
docs/              architecture and operations notes
```

## Build

Requires Go 1.22+ and ffmpeg for audio transcode.

```bash
make test
make build
# or:
go build ./cmd/station
go build ./cmd/relay
go build ./cmd/relay-agent
go build ./cmd/directory
```

## Run a station locally

```bash
RADIO_GO_ADDR=127.0.0.1:8091 \
RADIO_GO_ADMIN_TOKEN=change-me \
RADIO_LIBRARY_DIRS="$HOME/Music" \
go run ./cmd/station
```

Open <http://127.0.0.1:8091/>.

The default static directory is `web/static`. The optional commercial helper is
looked up at `plugins/commercial/helper.py`; without it the station still works
with the local library, but NetEase/QQ/Bilibili sources are unavailable.

## VPS role

**Rendezvous only is the intended deployment.** The directory and control
relay may run on the VPS. In the default `p2p` mode, audio comes from the
station host seed or peer relays over WebRTC; the VPS rejects media paths.

Read [docs/VPS_ROLE.md](docs/VPS_ROLE.md) and [docs/STREAMING.md](docs/STREAMING.md) before deploying. The relay defaults
to rejecting `/native/`, `/source/` and `/media/`; set `RELAY_ALLOW_MEDIA=1`
only if you explicitly want the relay to carry media (the opposite of the
rendezvous-only policy).

## Configuration

Common station variables:

```text
RADIO_GO_ADDR=127.0.0.1:8091
RADIO_GO_ADMIN_TOKEN=...
RADIO_LIBRARY_DIRS=/music:/more-music
RADIO_GO_STATE=data/station-go-state.json
RADIO_GO_UPLOADS=data/uploads
RADIO_STATIC_DIR=web/static
RADIO_PRIVATE_HELPER=plugins/commercial/helper.py
RADIO_DIRECTORY_URL=https://join.example.com
RADIO_DIRECTORY_REGISTER_TOKEN=...
RADIO_DIRECTORY_PUBLIC_URL=https://station.example.com   # direct station URL
RADIO_JOIN_REQUIRED=1
```

Relay variables:

```text
RELAY_TOKEN=...
RELAY_TLS_ADDR=:9443
RELAY_HTTP_ADDR=127.0.0.1:18081
RELAY_CERT=/path/fullchain.pem
RELAY_KEY=/path/privkey.pem
RELAY_ALLOW_MEDIA=0
```

Relay agent:

```text
RELAY_HOST=relay.example.com
RELAY_PORT=9443
RELAY_TOKEN=...
RELAY_LOCAL=127.0.0.1:8091
```

## Optional commercial helper

The helper is private, so `plugins/commercial/helper.py` and its private source
module are ignored by git. Copy them locally when building a package that needs
NetEase/QQ/Bilibili; release packaging includes the directory if present.

## Push this project

```bash
cd go-radio
git init -b main
git add .
git commit -m "initial radio-go project"
git remote add origin git@github.com:<you>/<repo>.git
git push -u origin main
```

The project has its own `go.mod` and can be pushed as a standalone repository.

## License

MIT, see `LICENSE`.
