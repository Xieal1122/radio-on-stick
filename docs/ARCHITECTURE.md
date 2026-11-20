# Architecture

```text
Browser / App listener
        |
        | direct HTTPS: web app, /source, /native, /ws
        v
  Station (this repo, cmd/station)
        |
        +-- local library + ffmpeg
        +-- optional commercial helper (plugins/commercial)
        +-- Directory registration
        +-- optional outbound relay agent

Directory (cmd/directory)  <-- invite code -> direct station URLs
Relay (cmd/relay)          <-- control/rendezvous only by default
```

The station is authoritative for the playback timeline, queue, history,
votes, listeners, messages and admin state. Browsers receive the same state
from `/now.json` and `/api/state`.

Media:

```text
P2P WebRTC DataChannel when available
otherwise direct station /native or /source
```

The relay is not an audio CDN. See [VPS_ROLE.md](VPS_ROLE.md).
