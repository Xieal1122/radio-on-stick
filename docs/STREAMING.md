# Streaming transport

## Why the old path stuttered

The original mesh sent raw RAD4 audio frames over an ordered SCTP
DataChannel and rebuilt a MediaSource timeline in every browser. That puts
jitter buffering, retransmission, congestion control and recovery policy in
application code. A single lost/blocked peer or a parent change can stall the
whole tree, and a 2 KB/s control/heartbeat connection looks alive while no
media arrives.

## Current transport

The Go station exposes its own WebRTC Opus track through Pion
(`internal/rtcseed`). `app/static/mesh.js` uses one WebRTC **audio RTP track**
per peer connection:

```text
Go server ffmpeg -> Opus RTP -> Pion track -> browser listener
browser listener <audio> --WebAudio destination--> RTP track -> child listener
```

- `MediaStreamAudioDestinationNode` gives one continuous track while the
  underlying `<audio>` element swaps songs.
- The browser's WebRTC stack handles jitter buffering, NACK/retransmission,
  congestion control and audio decoding.
- The existing WebSocket is only signaling/topology.
- `?tracks=0` remains as a diagnostic fallback to the legacy DataChannel path.

## Open-source references

- [Pion WebRTC](https://github.com/pion/webrtc) — Go WebRTC implementation and
  the same RTP/track model used by Go-native SFUs.
- [LiveKit](https://github.com/livekit/livekit) — production SFU that forwards
  RTP tracks and relies on browser/WebRTC congestion control rather than
  application frame queues.
- [mediasoup](https://mediasoup.org/) — widely deployed SFU with the same
  producer/consumer RTP model.
- [P2P Media Loader](https://github.com/Novage/p2p-media-loader) — useful for
  peer segment selection/caching ideas, but its segment-level P2P model still
  leaves media buffering to MSE/player code. Audio RTP is a better fit for a
  small radio mesh.

The implementation deliberately does not add Pion/mediasoup/LiveKit as a
server-side SFU: the VPS is rendezvous-only, so media must remain browser P2P.


## Preloading

The library queue is shuffled once when rebuilt and `next_auto` is exposed to
the client, so the next random track is stable. The client preloads the next
track's cover and lyrics only.

Audio itself is not preloaded in p2p mode: fetching `/native` or `/source`
before the switch would send media through the VPS, which is forbidden. The
RTP track has a continuous browser jitter buffer; avoiding a second HTTP audio
path is the deliberate tradeoff.
