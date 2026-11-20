(function () {
  "use strict";

  /*
   * Mesh audio transport.
   *
   * The server only exposes /source/manifest and /source/chunk.  Seeds fetch
   * those raw chunks and relay the exact frame (header + audio bytes) to their
   * children over DataChannels.  Every node forwards a valid frame before it
   * hands the payload to MediaSource; audio bytes are never re-encoded.
   */

  const HEADER_BYTES = 40;
  const MAGIC = 0x52414434; // RAD4
  const FORMAT_MIMES = {
    1: "audio/mpeg",
    2: "audio/aac",
    3: 'audio/mp4; codecs="mp4a.40.2"',
    4: "audio/ogg",
    5: "audio/webm; codecs=opus",
    6: "audio/wav",
    7: "audio/flac",
  };
  const FNV_OFFSET = 0x811c9dc5;
  const FNV_PRIME = 0x01000193;
  const LEAD_MS = 3200;
  const SYNC_TOLERANCE_MS = 700;
  const CATCHUP_MS = 10000;
  const CATCHUP_COOLDOWN_MS = 6000;
  const SOURCE_BUFFER_MS = 5200;
  const MAX_QUEUE_FRAMES = 120;
  const BUFFER_HIGH = 1024 * 1024;
  const BUFFER_LOW = 256 * 1024;
  const MAX_CHANNEL_QUEUE = 80;

  const audioEl = document.getElementById("audio");
  const audioModeEl = document.getElementById("audioMode");
  const params = new URLSearchParams(location.search);
  // WebRTC audio tracks (RTP) instead of application-level frame chunks on an
  // SCTP DataChannel. This is the transport used by Pion/mediasoup/LiveKit:
  // the browser owns jitter buffering, NACK/FEC and congestion control.
  const TRACK_MODE = params.get("tracks") !== "0";

  let preferredRole = "child";
  let canRelay = true;
  try {
    const stored = localStorage.getItem("radio.mesh.role");
    if (stored === "seed" || stored === "child" || stored === "leaf") {
      preferredRole = stored;
    }
    if (localStorage.getItem("radio.mesh.relay") === "0") canRelay = false;
  } catch (_) {
    /* storage can be unavailable */
  }
  const roleParam = params.get("role");
  if (roleParam === "seed" || roleParam === "child" || roleParam === "leaf") {
    preferredRole = roleParam;
    try {
      localStorage.setItem("radio.mesh.role", preferredRole);
    } catch (_) {
      /* ignore */
    }
  }
  const loopbackHost = ["127.0.0.1", "localhost", "::1", "[::1]"].includes(location.hostname);
  if (loopbackHost && !roleParam) {
    // The station initiator's own browser is the only legal HTTP media seed.
    // Remote browsers reached through a relay must never fetch /source.
    preferredRole = "seed";
  }
  if (preferredRole === "leaf" || params.get("relay") === "0") {
    canRelay = false;
    preferredRole = "leaf";
  }
  let stun = params.get("stun") || "";
  const rtcConfig = stun ? { iceServers: [{ urls: stun }] } : { iceServers: [] };

  let ws = null;
  let wsReady = false;
  let reconnectTimer = null;
  let reconnectDelay = 700;
  let meshFallbackTimer = null;
  let manualClose = false;
  let nodeId = null;
  let role = null;
  let active = false;
  let wanted = false;
  let userPaused = false;
  let trackSeq = 0;
  let trackId = "";
  let audioRev = 0;
  let nowSnapshot = null;
  let clockOffset = 0;
  let clockSamples = 0;
  let fallbackCallback = null;
  let fallbackReason = "";
  let disabledKey = "";
  let lastMode = "";
  let lastError = "";

  let accepted = freshAccepted();
  let currentStream = null;
  let media = null;
  let sourcePump = null;
  let sourceStartFailures = 0;
  let lastCatchupAt = 0;
  let lastResyncAt = 0;
  let stallTimes = [];
  let sourceToken = 0;
  let localAudioTrack = null;
  let localAudioStream = null;
  let audioContext = null;
  let audioSourceNode = null;
  let audioDestination = null;
  let activeAudioParent = null;
  let activeAudioStream = null;
  let liveCallback = null;

  const upstreams = new Map();
  const downstreams = new Map();
  const pendingSignals = new Map();

  function freshAccepted() {
    return {
      streamId: null,
      firstSeq: null,
      lastSeq: null,
      chunks: 0,
      dup: 0,
      gaps: 0,
      bad: 0,
      bytes: 0,
    };
  }

  const MSE_TYPES = [
    "audio/mpeg",
    'audio/mp4; codecs="mp4a.40.2"',
    "audio/webm; codecs=opus",
    "audio/ogg; codecs=opus",
    "audio/aac",
    "audio/flac",
    "audio/wav",
  ];

  function firstMseType() {
    if (!window.MediaSource) return "";
    for (const type of MSE_TYPES) {
      try {
        if (window.MediaSource.isTypeSupported(type)) return type;
      } catch (_) {
        /* keep looking */
      }
    }
    return "";
  }

  function mseSupported() {
    return Boolean(firstMseType());
  }

  function meshSupported() {
    return Boolean(
      mseSupported() && window.RTCPeerConnection && window.WebSocket
    );
  }

  function supported() {
    return meshSupported();
  }

  function sourceRole() {
    return role === "seed" || role === "http";
  }

  function finite(value, fallback) {
    const number = Number(value);
    return Number.isFinite(number) ? number : fallback;
  }

  function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  function debug(message, extra) {
    if (window.console && console.debug) {
      if (extra !== undefined) console.debug("[mesh] " + message, extra);
      else console.debug("[mesh] " + message);
    }
  }

  function send(message) {
    if (!ws || !wsReady || ws.readyState !== WebSocket.OPEN) return false;
    try {
      ws.send(JSON.stringify(message));
      return true;
    } catch (_) {
      return false;
    }
  }

  function sendSignal(to, payload) {
    return send({ type: "signal", to, payload });
  }

  function sampleClock(serverMs, sentAt, receivedAt) {
    const value = Number(serverMs);
    if (!Number.isFinite(value)) return;
    const offset = value - (sentAt + receivedAt) / 2;
    if (!clockSamples) {
      clockOffset = offset;
      clockSamples = 1;
    } else {
      clockOffset = clockOffset * 0.82 + offset * 0.18;
      clockSamples += 1;
    }
  }

  function serverNow() {
    return Date.now() + clockOffset;
  }

  function updateMode(force) {
    let text = "";
    if (active) {
      const parents = Array.from(upstreams.values()).filter((item) => item.open).length;
      const children = Array.from(downstreams.values()).filter((item) => item.open).length;
      const buffer = Math.max(0, Math.round(bufferAheadMs()));
      if (role === "seed") {
        text = `P2P 种子 · ${children} 下行 · 缓冲 ${buffer}ms`;
      } else if (role === "child") {
        text = `P2P 子节点 · ${parents} 上行 · 缓冲 ${buffer}ms`;
      } else if (role === "http") {
        text = `HTTP 直连 · 无 WebRTC · 缓冲 ${buffer}ms`;
      } else {
        text = "P2P 连接中";
      }
    }
    if (text === lastMode) return;
    lastMode = text;
    if (audioModeEl) audioModeEl.textContent = text || "直连";
  }

  function bufferAheadMs() {
    if (!media) return 0;
    let buffered = 0;
    try {
      if (media.buffer && media.buffer.buffered.length) {
        const end = media.buffer.buffered.end(media.buffer.buffered.length - 1);
        buffered = Math.max(0, (end - audioEl.currentTime) * 1000);
      }
    } catch (_) {
      buffered = 0;
    }
    return buffered + (media.pendingMs || 0);
  }

  /* ----------------------------- media ----------------------------- */

  function resetMedia() {
    if (media) {
      try {
        if (media.buffer && media.source && media.source.readyState === "open") {
          if (media.buffer.updating) media.buffer.abort();
        }
      } catch (_) {
        /* ignore */
      }
      try {
        if (media.objectUrl) URL.revokeObjectURL(media.objectUrl);
      } catch (_) {
        /* ignore */
      }
      media = null;
    }
    try {
      audioEl.pause();
      audioEl.removeAttribute("src");
      audioEl.load();
    } catch (_) {
      /* ignore */
    }
  }

  function setupMedia(mime) {
    const type = mime || "audio/mpeg";
    if (
      !window.MediaSource ||
      !window.MediaSource.isTypeSupported(type)
    ) {
      return false;
    }
    resetMedia();
    const source = new MediaSource();
    const objectUrl = URL.createObjectURL(source);
    media = {
      source,
      objectUrl,
      buffer: null,
      queue: [],
      pendingMs: 0,
      started: false,
      failed: false,
      eof: false,
      mime: type,
    };
    // Attach sourceopen before load(): the event can fire immediately when the
    // element already has a MediaSource URL, especially in background tabs.
    source.addEventListener(
      "sourceopen",
      () => {
        if (!media || media.source !== source) return;
        try {
          const buffer = source.addSourceBuffer(media.mime);
          try {
            // MP3 uses generated timestamps; "segments" is not valid here.
            buffer.mode = "sequence";
          } catch (_) {
            /* the default mode is already usable */
          }
          buffer.addEventListener("updateend", pumpMedia);
          media.buffer = buffer;
          pumpMedia();
        } catch (error) {
          debug("addSourceBuffer failed", error);
          failMesh("mse");
        }
      },
      { once: true }
    );
    audioEl.src = objectUrl;
    audioEl.load();
    return true;
  }

  function pumpMedia() {
    if (!media || !media.buffer || media.buffer.updating) return;
    if (!media.queue.length) return;
    if (!media.started) {
      const waitingForLead =
        !media.eof && bufferAheadMs() < LEAD_MS && media.queue.length < 4;
      if (waitingForLead) return;
    }
    const item = media.queue.shift();
    media.pendingMs = Math.max(0, media.pendingMs - item.duration);
    try {
      media.buffer.appendBuffer(item.data);
      media.started = true;
      debug("append", `${item.duration}ms queued=${media.queue.length}`);
    } catch (error) {
      if (error && error.name === "QuotaExceededError") {
        media.queue.unshift(item);
        media.pendingMs += item.duration;
        try {
          const current = audioEl.currentTime || 0;
          const start = Math.max(0, current - 2);
          media.buffer.remove(start, current + 4);
        } catch (_) {
          failMesh("mse-quota");
        }
      } else {
        debug("appendBuffer failed", error);
        failMesh("mse");
      }
    }
  }

  function queuePayload(payload, meta) {
    if (!active || !wanted) return;
    const mime = meta.mime || "audio/mpeg";
    if (!media || media.mime !== mime) {
      if (!setupMedia(mime)) {
        failMesh("unsupported");
        return;
      }
    }
    if (!media) return;
    const data = payload.buffer.slice(payload.byteOffset, payload.byteOffset + payload.byteLength);
    media.queue.push({ data, duration: Math.max(1, meta.durationMs) });
    media.pendingMs += Math.max(1, meta.durationMs);
    if (media.queue.length > MAX_QUEUE_FRAMES) {
      const dropped = media.queue.shift();
      media.pendingMs = Math.max(0, media.pendingMs - dropped.duration);
    }
    pumpMedia();
  }

  function mediaPositionMs() {
    if (!currentStream) return null;
    return currentStream.firstStartMs + audioEl.currentTime * 1000;
  }

  function syncTick() {
    if (!active || !currentStream || !media || !media.started) return;
    if (audioEl.paused || audioEl.ended) return;
    const target = currentStream.firstStartMs + (serverNow() - currentStream.firstServerMs);
    let end = null;
    try {
      if (media.buffer && media.buffer.buffered.length) {
        end = media.buffer.buffered.end(media.buffer.buffered.length - 1);
      }
    } catch (_) {
      end = null;
    }
    if (end !== null) {
      const targetSeconds = (target - currentStream.firstStartMs) / 1000;
      const current = audioEl.currentTime;
      if (
        targetSeconds > end + CATCHUP_MS / 1000 &&
        Date.now() - lastCatchupAt > CATCHUP_COOLDOWN_MS
      ) {
        // The device fell behind the live edge by more than the buffer can
        // bridge (slow start, suspended tab, or a long stall).  Jump to the
        // current live edge instead of staying seconds behind forever.
        lastCatchupAt = Date.now();
        if (sourceRole()) {
          // The serving node is already the live source. Rebuilding its
          // MediaSource here causes the audible 0 -> live jumps users see.
          return;
        } else {
          const mime = currentStream ? currentStream.mime : "audio/mpeg";
          accepted = freshAccepted();
          currentStream = null;
          if (!setupMedia(mime)) failMesh("mse");
        }
        return;
      }
      if (
        targetSeconds > current + SYNC_TOLERANCE_MS / 1000 &&
        targetSeconds < end - 0.15
      ) {
        try {
          audioEl.currentTime = targetSeconds;
        } catch (_) {
          /* ignore */
        }
      }
    }
  }

  /* --------------------------- data channel --------------------------- */

  function fnv1a(bytes, start, length) {
    let value = FNV_OFFSET;
    const end = start + length;
    for (let index = start; index < end; index += 1) {
      value ^= bytes[index];
      value = Math.imul(value, FNV_PRIME) >>> 0;
    }
    return value >>> 0;
  }

  function parseFrame(raw) {
    if (!(raw instanceof ArrayBuffer) || raw.byteLength < HEADER_BYTES) return null;
    const view = new DataView(raw);
    if (view.getUint32(0) !== MAGIC) return null;
    const streamId = view.getUint32(4);
    const seq = view.getUint32(8);
    const payloadLen = view.getUint32(12);
    const startMs = view.getUint32(16);
    const durationMs = view.getUint32(20);
    const serverMs = view.getFloat64(24);
    const checksum = view.getUint32(32);
    const formatCode = view.getUint8(36);
    if (payloadLen > 256 * 1024 || HEADER_BYTES + payloadLen !== raw.byteLength) return null;
    const bytes = new Uint8Array(raw, HEADER_BYTES, payloadLen);
    if (fnv1a(bytes, 0, payloadLen) !== checksum) return null;
    return {
      streamId,
      seq,
      payloadLen,
      startMs,
      durationMs,
      serverMs,
      formatCode,
      mime: FORMAT_MIMES[formatCode] || "audio/mpeg",
      payload: bytes,
    };
  }

  function receiveFrame(raw, fromId) {
    const frame = parseFrame(raw);
    if (!frame) {
      accepted.bad += 1;
      lastError = "bad_frame";
      return;
    }
    const link = upstreams.get(fromId);
    if (link) {
      link.chunks += 1;
      link.bytes += raw.byteLength;
      link.lastSeq = frame.seq;
    }

    if (accepted.streamId !== frame.streamId) {
      const hadStream = accepted.streamId !== null;
      accepted = freshAccepted();
      accepted.streamId = frame.streamId;
      currentStream = {
        id: frame.streamId,
        firstStartMs: frame.startMs,
        firstServerMs: Number.isFinite(frame.serverMs) ? frame.serverMs : serverNow(),
        lastDurationMs: frame.durationMs,
        mime: frame.mime,
      };
      if (hadStream && active) {
        // A new track on the same page: discard old media and start clean.
        if (!setupMedia(frame.mime)) {
          failMesh("mse");
          return;
        }
      }
      debug("stream", `${frame.streamId} first=${frame.seq}`);
    }

    if (accepted.lastSeq !== null && frame.seq <= accepted.lastSeq) {
      accepted.dup += 1;
      return;
    }
    if (accepted.lastSeq !== null && frame.seq > accepted.lastSeq + 1) {
      accepted.gaps += 1;
      debug("gap", `${accepted.lastSeq} -> ${frame.seq}`);
    }
    if (accepted.firstSeq === null) accepted.firstSeq = frame.seq;
    accepted.lastSeq = frame.seq;
    accepted.chunks += 1;
    accepted.bytes += raw.byteLength;
    currentStream.lastDurationMs = frame.durationMs;

    // The seed's local audio element is the RTP source for downstreams.
    if (TRACK_MODE) ensureLocalAudioTrack();
    // Relay the original bytes before handing them to the media pipeline.
    forwardFrame(raw);
    queuePayload(frame.payload, frame);
    updateMode();
  }

  function enqueueDownstream(link, raw) {
    if (!link || !link.dc) return;
    if (link.dc.readyState !== "open") {
      if (link.queue.length < MAX_CHANNEL_QUEUE) link.queue.push(raw);
      return;
    }
    if (link.dc.bufferedAmount < BUFFER_HIGH) {
      try {
        link.dc.send(raw);
        return;
      } catch (_) {
        /* queue below */
      }
    }
    if (link.queue.length >= MAX_CHANNEL_QUEUE) link.queue.shift();
    link.queue.push(raw);
  }

  function pumpDownstream(link) {
    if (!link || !link.dc || link.dc.readyState !== "open") return;
    while (link.queue.length && link.dc.bufferedAmount < BUFFER_LOW) {
      const raw = link.queue.shift();
      try {
        link.dc.send(raw);
      } catch (_) {
        link.queue.unshift(raw);
        break;
      }
    }
  }

  function ensureLocalAudioTrack() {
    if (!TRACK_MODE) return null;
    if (localAudioTrack && localAudioTrack.readyState === "live") {
      return localAudioTrack;
    }
    // A MediaStreamAudioDestinationNode gives us one continuous RTP track
    // while the underlying <audio> element swaps songs. captureStream() can
    // end its track on every source change, which breaks a long P2P session.
    try {
      if (!audioContext && (window.AudioContext || window.webkitAudioContext)) {
        const Ctx = window.AudioContext || window.webkitAudioContext;
        audioContext = new Ctx();
        audioSourceNode = audioContext.createMediaElementSource(audioEl);
        audioDestination = audioContext.createMediaStreamDestination();
        audioSourceNode.connect(audioDestination);
        audioSourceNode.connect(audioContext.destination);
        audioContext.resume().catch(() => {});
      }
      if (audioDestination) {
        localAudioStream = audioDestination.stream;
        localAudioTrack = localAudioStream.getAudioTracks()[0] || null;
      }
    } catch (_) {
      audioContext = null;
      audioSourceNode = null;
      audioDestination = null;
    }
    if (!localAudioTrack) {
      try {
        const stream = audioEl.captureStream
          ? audioEl.captureStream()
          : audioEl.mozCaptureStream
            ? audioEl.mozCaptureStream()
            : null;
        localAudioStream = stream || null;
        localAudioTrack = stream && stream.getAudioTracks
          ? stream.getAudioTracks()[0] || null
          : null;
      } catch (_) {
        localAudioTrack = null;
      }
    }
    if (localAudioTrack) {
      for (const link of downstreams.values()) {
        if (link.audioSender) {
          link.audioSender.replaceTrack(localAudioTrack).catch(() => {});
        }
      }
    }
    return localAudioTrack;
  }

  function markLive() {
    if (typeof liveCallback === "function") {
      try {
        liveCallback();
      } catch (_) {
        /* UI callback is best-effort */
      }
    }
  }

  function selectAudioParent() {
    if (!TRACK_MODE) return;
    let selected = activeAudioParent ? upstreams.get(activeAudioParent) : null;
    if (!selected || !selected.audioStream) {
      selected = null;
      for (const link of upstreams.values()) {
        if (link.audioStream && link.pc.connectionState !== "failed") {
          selected = link;
          break;
        }
      }
    }
    if (!selected) {
      if (activeAudioParent) {
        activeAudioParent = null;
        activeAudioStream = null;
        try {
          audioEl.srcObject = null;
        } catch (_) {
          /* ignore */
        }
        updateMode();
      }
      return;
    }
    if (activeAudioParent === selected.id && activeAudioStream === selected.audioStream) {
      ensureLocalAudioTrack();
      return;
    }
    activeAudioParent = selected.id;
    activeAudioStream = selected.audioStream;
    try {
      audioEl.pause();
      audioEl.removeAttribute("src");
      audioEl.srcObject = selected.audioStream;
      audioEl.load();
      audioEl.play().catch(() => {
        /* the user gesture/attemptPlay path retries this */
      });
    } catch (_) {
      /* keep the old media if this browser cannot attach a track */
    }
    ensureLocalAudioTrack();
    markLive();
    updateMode();
  }

  function forwardFrame(raw) {
    // In RTP track mode the browser's RTP stack forwards media; SCTP frames
    // must not duplicate the same audio.
    if (TRACK_MODE) return;
    if (!downstreams.size) return;
    for (const link of downstreams.values()) enqueueDownstream(link, raw);
  }

  function createServerUpstream() {
    if (upstreams.has("server")) return upstreams.get("server");
    const pc = new RTCPeerConnection(rtcConfig);
    const link = {
      id: "server",
      pc,
      dc: null,
      open: false,
      pendingIce: [],
      chunks: 0,
      bytes: 0,
      lastSeq: null,
      closed: false,
      reportTimer: null,
      audioTrack: null,
      audioStream: null,
    };
    upstreams.set("server", link);
    pc.ontrack = (event) => {
      link.audioTrack = event.track || null;
      link.audioStream = event.streams && event.streams[0]
        ? event.streams[0]
        : event.track
          ? new MediaStream([event.track])
          : null;
      link.open = true;
      selectAudioParent();
    };
    pc.onconnectionstatechange = () => {
      if (pc.connectionState === "failed" || pc.connectionState === "closed") {
        link.open = false;
        if (!link.closed) reportUpstreamLost("server");
      }
    };
    try {
      pc.addTransceiver("audio", { direction: "recvonly" });
    } catch (error) {
      debug("server audio transceiver failed", error);
    }
    const waitIce = () => new Promise((resolve) => {
      if (pc.iceGatheringState === "complete") {
        resolve();
        return;
      }
      const check = () => {
        if (pc.iceGatheringState === "complete") {
          pc.removeEventListener("icegatheringstatechange", check);
          resolve();
        }
      };
      pc.addEventListener("icegatheringstatechange", check);
      setTimeout(resolve, 3500);
    });
    pc.createOffer()
      .then((offer) => pc.setLocalDescription(offer))
      .then(waitIce)
      .then(async () => {
        if (!pc.localDescription) throw new Error("missing local description");
        const response = await fetch("/api/server-rtc/offer", {
          method: "POST",
          cache: "no-store",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ sdp: pc.localDescription.sdp }),
        });
        if (!response.ok) throw new Error(`server rtc ${response.status}`);
        const answer = await response.json();
        await pc.setRemoteDescription({ type: "answer", sdp: answer.sdp });
      })
      .catch((error) => {
        debug("server upstream failed", error);
        link.closed = true;
        try {
          pc.close();
        } catch (_) {
          /* ignore */
        }
        upstreams.delete("server");
      });
    return link;
  }

  function createUpstream(parentId) {
    if (parentId === "server") return createServerUpstream();
    if (upstreams.has(parentId)) return upstreams.get(parentId);
    const pc = new RTCPeerConnection(rtcConfig);
    const link = {
      id: parentId,
      pc,
      dc: null,
      open: false,
      pendingIce: [],
      chunks: 0,
      bytes: 0,
      lastSeq: null,
      closed: false,
      reportTimer: null,
    };
    upstreams.set(parentId, link);
    if (TRACK_MODE) {
      pc.ontrack = (event) => {
        link.audioTrack = event.track || null;
        link.audioStream = event.streams && event.streams[0]
          ? event.streams[0]
          : event.track
            ? new MediaStream([event.track])
            : null;
        selectAudioParent();
      };
    }
    pc.ondatachannel = (event) => {
      const dc = event.channel;
      link.dc = dc;
      dc.binaryType = "arraybuffer";
      dc.onopen = () => {
        link.open = true;
        updateMode();
      };
      dc.onmessage = (message) => {
        if (message.data instanceof ArrayBuffer) receiveFrame(message.data, parentId);
      };
      dc.onclose = () => {
        link.open = false;
        if (!link.closed) reportUpstreamLost(parentId);
      };
      dc.onerror = () => {
        /* close event follows */
      };
    };
    pc.onicecandidate = (event) => {
      if (event.candidate) {
        const candidate =
          typeof event.candidate.toJSON === "function"
            ? event.candidate.toJSON()
            : event.candidate;
        sendSignal(parentId, { kind: "ice", candidate });
      }
    };
    pc.onconnectionstatechange = () => {
      if (pc.connectionState === "failed") {
        link.open = false;
        if (!link.closed) reportUpstreamLost(parentId);
      }
    };
    const queued = pendingSignals.get(parentId);
    if (queued) {
      pendingSignals.delete(parentId);
      for (const payload of queued) handleSignal(parentId, payload);
    }
    return link;
  }

  function createDownstream(childId) {
    if (!canRelay) return null;
    if (downstreams.has(childId)) return downstreams.get(childId);
    const pc = new RTCPeerConnection(rtcConfig);
    const link = {
      id: childId,
      pc,
      dc: null,
      open: false,
      pendingIce: [],
      queue: [],
      closed: false,
      reportTimer: null,
    };
    downstreams.set(childId, link);
    if (TRACK_MODE) {
      try {
        const transceiver = pc.addTransceiver("audio", { direction: "sendrecv" });
        link.audioSender = transceiver.sender;
        const track = ensureLocalAudioTrack();
        if (track) link.audioSender.replaceTrack(track).catch(() => {});
      } catch (error) {
        debug("audio transceiver failed", error);
      }
    }
    // A paused seed must still fetch and forward for its children.
    if (active && role === "seed" && !sourcePump) startSource();
    pc.onicecandidate = (event) => {
      if (event.candidate) {
        const candidate =
          typeof event.candidate.toJSON === "function"
            ? event.candidate.toJSON()
            : event.candidate;
        sendSignal(childId, { kind: "ice", candidate });
      }
    };
    pc.onconnectionstatechange = () => {
      if (pc.connectionState === "failed") {
        link.open = false;
        if (!link.closed) reportDownstreamLost(childId);
      }
    };
    try {
      const dc = pc.createDataChannel("audio", { ordered: true });
      link.dc = dc;
      dc.binaryType = "arraybuffer";
      dc.bufferedAmountLowThreshold = BUFFER_LOW;
      dc.onopen = () => {
        link.open = true;
        pumpDownstream(link);
        updateMode();
      };
      dc.onbufferedamountlow = () => pumpDownstream(link);
      dc.onclose = () => {
        link.open = false;
        if (!link.closed) reportDownstreamLost(childId);
      };
      dc.onerror = () => {
        /* close event follows */
      };
    } catch (error) {
      debug("createDataChannel failed", error);
      removeDownstream(childId, false);
      return link;
    }
    pc.createOffer()
      .then((offer) => pc.setLocalDescription(offer))
      .then(() => {
        sendSignal(childId, { kind: "offer", sdp: pc.localDescription.sdp });
      })
      .catch((error) => {
        debug("downstream offer failed", error);
        removeDownstream(childId, false);
      });
    return link;
  }

  function reportUpstreamLost(parentId) {
    const link = upstreams.get(parentId);
    if (!link || link.closed) return;
    link.closed = true;
    try {
      link.pc.close();
    } catch (_) {
      /* ignore */
    }
    upstreams.delete(parentId);
    if (activeAudioParent === parentId) selectAudioParent();
    send({ type: "upstream-lost", parent_id: parentId });
    if (active && role === "child" && openParentCount() < 2) armMeshFallbackTimer();
    updateMode();
  }

  function reportDownstreamLost(childId) {
    const link = downstreams.get(childId);
    if (!link || link.closed) return;
    link.closed = true;
    try {
      link.pc.close();
    } catch (_) {
      /* ignore */
    }
    downstreams.delete(childId);
    send({ type: "downstream-lost", child_id: childId });
    updateMode();
  }

  function removeUpstream(parentId, report) {
    const link = upstreams.get(parentId);
    pendingSignals.delete(parentId);
    if (!link) return;
    link.closed = true;
    try {
      link.pc.close();
    } catch (_) {
      /* ignore */
    }
    upstreams.delete(parentId);
    if (activeAudioParent === parentId) selectAudioParent();
    if (report) send({ type: "upstream-lost", parent_id: parentId });
    if (active && role === "child" && openParentCount() < 2) armMeshFallbackTimer();
    updateMode();
  }

  function removeDownstream(childId, report) {
    const link = downstreams.get(childId);
    pendingSignals.delete(childId);
    if (!link) return;
    link.closed = true;
    try {
      link.pc.close();
    } catch (_) {
      /* ignore */
    }
    downstreams.delete(childId);
    if (report) send({ type: "downstream-lost", child_id: childId });
    updateMode();
  }

  function closeAllPeers() {
    for (const id of Array.from(upstreams.keys())) removeUpstream(id, false);
    for (const id of Array.from(downstreams.keys())) removeDownstream(id, false);
  }

  async function handleSignal(fromId, payload) {
    if (!payload || typeof payload !== "object") return;
    const upstream = upstreams.get(fromId);
    const downstream = downstreams.get(fromId);
    const link = upstream || downstream;
    if (!link) {
      const queue = pendingSignals.get(fromId) || [];
      if (queue.length < 20) queue.push(payload);
      pendingSignals.set(fromId, queue);
      return;
    }
    try {
      if (payload.kind === "offer" && upstream) {
        await upstream.pc.setRemoteDescription({ type: "offer", sdp: payload.sdp });
        await flushIce(upstream);
        const answer = await upstream.pc.createAnswer();
        await upstream.pc.setLocalDescription(answer);
        sendSignal(fromId, { kind: "answer", sdp: upstream.pc.localDescription.sdp });
      } else if (payload.kind === "answer" && downstream) {
        await downstream.pc.setRemoteDescription({ type: "answer", sdp: payload.sdp });
        await flushIce(downstream);
      } else if (payload.kind === "ice" && payload.candidate) {
        if (link.pc.remoteDescription) {
          await link.pc.addIceCandidate(payload.candidate);
        } else {
          link.pendingIce.push(payload.candidate);
        }
      }
    } catch (error) {
      debug("signal failed", error);
    }
  }

  async function flushIce(link) {
    const pending = link.pendingIce.splice(0);
    for (const candidate of pending) {
      try {
        await link.pc.addIceCandidate(candidate);
      } catch (error) {
        debug("ice failed", error);
      }
    }
  }

  /* ----------------------------- source ----------------------------- */

  function stopSource() {
    sourceToken += 1;
    if (sourcePump) {
      try {
        if (sourcePump.abort) sourcePump.abort.abort();
      } catch (_) {
        /* ignore */
      }
    }
    sourcePump = null;
  }

  function sourceRequestHeaders() {
    return role === "http" ? { "X-Radio-Source-Role": "http" } : undefined;
  }

  async function fetchManifest(token) {
    const sent = Date.now();
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 12000);
    let response;
    try {
      response = await fetch(`/source/manifest?seq=${encodeURIComponent(trackSeq)}`, {
        cache: "no-store",
        headers: sourceRequestHeaders(),
        signal: controller.signal,
      });
    } finally {
      clearTimeout(timer);
    }
    const received = Date.now();
    if (response.status === 409) throw new Error("stale");
    if (response.status === 415) throw new Error("unsupported");
    if (!response.ok) {
      const error = new Error(`manifest ${response.status}`);
      error.status = response.status;
      throw error;
    }
    const manifest = await response.json();
    if (token !== sourceToken) return null;
    sampleClock(manifest.server_time_ms, sent, received);
    return manifest;
  }

  async function fetchChunk(ctx, seq) {
    const controller = new AbortController();
    ctx.abort = controller;
    const timer = setTimeout(() => controller.abort(), 15000);
    try {
      const response = await fetch(
        `/source/chunk?stream_id=${encodeURIComponent(ctx.manifest.stream_id)}&seq=${encodeURIComponent(seq)}`,
        {
          cache: "no-store",
          headers: sourceRequestHeaders(),
          signal: controller.signal,
        }
      );
      if (response.status === 410 || response.status === 409) {
        const error = new Error("gone");
        error.status = response.status;
        throw error;
      }
      if (!response.ok) {
        const error = new Error(`chunk ${response.status}`);
        error.status = response.status;
        throw error;
      }
      return await response.arrayBuffer();
    } finally {
      clearTimeout(timer);
      ctx.abort = null;
    }
  }

  async function runSource(ctx) {
    let failures = 0;
    while (
      active &&
      sourceRole() &&
      sourcePump === ctx &&
      ctx.token === sourceToken
    ) {
      const servingChildren = downstreams.size > 0;
      if (!wanted && !servingChildren && !ctx.eof) {
        await sleep(250);
        continue;
      }
      if (!servingChildren && bufferAheadMs() >= SOURCE_BUFFER_MS && !ctx.eof) {
        await sleep(220);
        continue;
      }
      if (ctx.manifest && ctx.manifest.eof && ctx.nextSeq > ctx.manifest.latest_seq) {
        ctx.eof = true;
        if (media) media.eof = true;
        pumpMedia();
        return;
      }
      const seq = ctx.nextSeq;
      try {
        const raw = await fetchChunk(ctx, seq);
        if (sourcePump !== ctx || ctx.token !== sourceToken) return;
        if (!raw) throw new Error("empty chunk");
        receiveFrame(raw, "source");
        ctx.nextSeq = seq + 1;
        failures = 0;
      } catch (error) {
        if (sourcePump !== ctx || ctx.token !== sourceToken) return;
        if (error && (error.status === 410 || error.status === 409)) {
          debug("source stream reset", error.status);
          restartSource();
          return;
        }
        if (error && error.status === 429) {
          debug("http leaf admission rejected");
          failMesh("leaf-busy");
          return;
        }
        if (error && error.status === 503) {
          // The producer may have reached the end after the initial manifest.
          try {
            const latest = await fetchManifest(sourceToken);
            if (
              latest &&
              latest.eof &&
              ctx.nextSeq > latest.latest_seq
            ) {
              ctx.eof = true;
              if (media) media.eof = true;
              pumpMedia();
              return;
            }
          } catch (_) {
            /* fall through to the retry path */
          }
        }
        failures += 1;
        if (failures >= 4) {
          debug("source failed", error);
          failMesh("source");
          return;
        }
        await sleep(Math.min(2500, 350 * failures));
      }
    }
  }

  async function startSource() {
    if (!active || !sourceRole()) return;
    stopSource();
    const token = sourceToken;
    const ctx = {
      token,
      manifest: null,
      nextSeq: 0,
      abort: null,
      eof: false,
    };
    sourcePump = ctx;
    try {
      const manifest = await fetchManifest(token);
      if (!manifest || sourcePump !== ctx || ctx.token !== sourceToken) return;
      if (manifest.paused) {
        sourcePump = null;
        return;
      }
      if (!manifest.supported) throw new Error("unsupported");
      const mime = manifest.mime || "audio/mpeg";
      if (!window.MediaSource || !window.MediaSource.isTypeSupported(mime)) {
        throw new Error("unsupported");
      }
      if (!media || media.mime !== mime) {
        if (!setupMedia(mime)) throw new Error("unsupported");
      }
      sourceStartFailures = 0;
      ctx.manifest = manifest;
      if (manifest.eof && manifest.latest_seq < 0) {
        ctx.eof = true;
        if (media) media.eof = true;
        return;
      }
      let next = finite(manifest.latest_seq, -1);
      if (next < 0) next = finite(manifest.from_seq, 0);
      if (accepted.streamId === manifest.stream_id && accepted.lastSeq !== null) {
        next = Math.max(next, accepted.lastSeq + 1);
      }
      ctx.nextSeq = next;
      debug("source", `stream=${manifest.stream_id} from=${next}`);
      await runSource(ctx);
    } catch (error) {
      if (sourcePump !== ctx || ctx.token !== sourceToken) return;
      sourcePump = null;
      debug("start source failed", error);
      if (error && error.message === "unsupported") {
        failMesh("unsupported");
        return;
      }
      if (error && error.status === 429) {
        failMesh("leaf-busy");
        return;
      }
      sourceStartFailures += 1;
      if (sourceStartFailures >= 4) {
        failMesh("source");
        return;
      }
      if (active && sourceRole()) {
        setTimeout(() => {
          if (active && sourceRole() && !sourcePump) startSource();
        }, 1200);
      }
    }
  }

  function restartSource() {
    if (!active || !sourceRole()) return;
    stopSource();
    sourceStartFailures = 0;
    // The old ring edge is gone (410) or the session was recreated.  Drop
    // the stale MSE timeline instead of appending a discontinuous jump.
    const mime = currentStream ? currentStream.mime : "audio/mpeg";
    accepted = freshAccepted();
    currentStream = null;
    if (!setupMedia(mime)) {
      failMesh("mse");
      return;
    }
    setTimeout(() => {
      if (active && sourceRole() && !sourcePump) startSource();
    }, 400);
  }

  /* ---------------------------- lifecycle ---------------------------- */

  function clearMeshFallbackTimer() {
    if (meshFallbackTimer) {
      clearTimeout(meshFallbackTimer);
      meshFallbackTimer = null;
    }
  }

  function enableHttpLeaf(reason) {
    if (!active || role === "http") return;
    clearMeshFallbackTimer();
    manualClose = true;
    if (ws) {
      const old = ws;
      ws = null;
      wsReady = false;
      try {
        old.close();
      } catch (_) {
        /* ignore */
      }
    }
    closeAllPeers();
    role = "http";
    nodeId = null;
    fallbackReason = reason || "http-leaf";
    lastError = fallbackReason;
    sourceStartFailures = 0;
    updateMode(true);
    if (!sourcePump) startSource();
  }

  function openParentCount() {
    return Array.from(upstreams.values()).filter((item) => item.open).length;
  }

  function armMeshFallbackTimer() {
    // P2P-only mode has no legal HTTP media path. A child with one parent is
    // valid and must keep that parent; never switch to the server HTTP leaf.
    clearMeshFallbackTimer();
  }

  function applyRole(nextRole) {
    if (nextRole !== "seed" && nextRole !== "child") return;
    const previous = role;
    role = nextRole;
    if (role === "seed") clearMeshFallbackTimer();
    else armMeshFallbackTimer();
    if (role === "seed") {
      for (const id of Array.from(upstreams.keys())) removeUpstream(id, false);
      if (active && (wanted || downstreams.size > 0)) startSource();
    } else if (role === "child") {
      stopSource();
    }
    if (previous !== role) debug("role", role);
    updateMode();
  }

  function ensureSocket() {
    if (manualClose) return;
    if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;
    const scheme = location.protocol === "https:" ? "wss" : "ws";
    try {
      ws = new WebSocket(`${scheme}://${location.host}/ws`);
    } catch (error) {
      debug("websocket failed", error);
      scheduleReconnect();
      return;
    }
    ws.onopen = () => {
      wsReady = true;
      reconnectDelay = 700;
      send({
        type: "hello",
        preferred_role: preferredRole,
        can_relay: canRelay,
      });
    };
    ws.onmessage = (event) => {
      let message = null;
      try {
        message = JSON.parse(event.data);
      } catch (_) {
        return;
      }
      if (!message || typeof message !== "object") return;
      switch (message.type) {
        case "welcome":
          nodeId = message.id;
          applyRole(message.role);
          break;
        case "role":
          applyRole(message.role);
          break;
        case "parent-added":
          if (message.parent_id && role !== "seed") createUpstream(message.parent_id);
          break;
        case "parent-removed":
          if (message.parent_id) removeUpstream(message.parent_id, false);
          break;
        case "child-joined":
          if (message.child_id) createDownstream(message.child_id);
          break;
        case "child-left":
          if (message.child_id) removeDownstream(message.child_id, false);
          break;
        case "signal":
          if (message.from && message.payload) handleSignal(message.from, message.payload);
          break;
        case "waiting":
          debug("waiting for parent");
          break;
        case "error":
          lastError = String(message.code || message.message || "mesh_error");
          debug("server error", message);
          break;
        default:
          break;
      }
    };
    ws.onerror = () => {
      /* close follows */
    };
    ws.onclose = () => {
      wsReady = false;
      ws = null;
      if (role === "http") return;
      nodeId = null;
      role = null;
      closeAllPeers();
      updateMode(true);
      scheduleReconnect();
    };
  }

  function scheduleReconnect() {
    if (manualClose || reconnectTimer) return;
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      ensureSocket();
    }, reconnectDelay);
    reconnectDelay = Math.min(6000, reconnectDelay + 600);
  }

  function noteStall() {
    if (!active || !wanted || userPaused || !media || !media.started) return;
    const now = Date.now();
    stallTimes = stallTimes.filter((stamp) => now - stamp < 60_000);
    stallTimes.push(now);
    if (stallTimes.length >= 4) {
      // Edge/Chromium can append MP3 MSE frames but still underrun under
      // audio-output timing pressure. Once this repeats, HTTP native playback
      // is smoother than repeatedly stuttering the P2P media path.
      failMesh("mse-unstable");
    }
  }

  function failMesh(reason) {
    if (!active) return;
    active = false;
    wanted = false;
    fallbackReason = reason;
    lastError = reason;
    disabledKey = `${trackSeq}:${audioRev}`;
    stopSource();
    resetMedia();
    currentStream = null;
    accepted = freshAccepted();
    leaveMesh();
    updateMode(true);
    debug("fallback", reason);
    if (typeof fallbackCallback === "function") {
      try {
        fallbackCallback(reason);
      } catch (error) {
        debug("fallback callback failed", error);
      }
    }
  }

  function start(nextNow, positionMs) {
    if (!mseSupported()) return false;
    const nextSeq = nextNow ? finite(nextNow.seq, 0) : 0;
    const nextRev = nextNow ? finite(nextNow.audio_rev, 0) : 0;
    if (disabledKey === `${nextSeq}:${nextRev}`) return false;
    if (active && nextNow && nextSeq === trackSeq && nextRev === audioRev) {
      return true;
    }
    stopSource();
    resetMedia();
    sourceStartFailures = 0;
    lastCatchupAt = 0;
    active = true;
    wanted = true;
    userPaused = false;
    accepted = freshAccepted();
    currentStream = null;
    fallbackReason = "";
    trackSeq = nextSeq;
    trackId = nextNow && nextNow.track ? String(nextNow.track.id || "") : "";
    audioRev = nextRev;
    nowSnapshot = nextNow || nowSnapshot;
    if (nextNow && nextNow.server_time_ms) {
      sampleClock(nextNow.server_time_ms, Date.now(), Date.now());
    }
    if (!stun && nextNow && nextNow.mesh && nextNow.mesh.stun) {
      stun = String(nextNow.mesh.stun);
      rtcConfig.iceServers = [{ urls: stun }];
    }
    if (!setupMedia(firstMseType())) {
      failMesh("unsupported");
      return false;
    }
    if (TRACK_MODE) ensureLocalAudioTrack();
    if (!meshSupported()) {
      // VPS media is disabled. A browser without WebRTC cannot receive P2P
      // audio; fail explicitly instead of pulling /source through the relay.
      failMesh("webrtc_unsupported");
      return false;
    }
    manualClose = false;
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      role = null;
      nodeId = null;
    }
    ensureSocket();
    if (role) applyRole(role);
    else armMeshFallbackTimer();
    updateMode(true);
    return true;
  }

  function leaveMesh() {
    manualClose = true;
    clearMeshFallbackTimer();
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
    closeAllPeers();
    if (ws) {
      try {
        ws.close();
      } catch (_) {
        /* ignore */
      }
    }
    ws = null;
    wsReady = false;
    role = null;
    nodeId = null;
    reconnectDelay = 700;
  }

  function stop() {
    active = false;
    wanted = false;
    userPaused = false;
    stopSource();
    resetMedia();
    currentStream = null;
    accepted = freshAccepted();
    leaveMesh();
    updateMode(true);
  }

  function update(nextNow) {
    if (!nextNow) return;
    nowSnapshot = nextNow;
    if (nextNow.server_time_ms) {
      sampleClock(nextNow.server_time_ms, Date.now(), Date.now());
    }
    if (!active) return;
    if (!nextNow.track || nextNow.paused) {
      stopSource();
      if (nextNow.paused) {
        try {
          audioEl.pause();
        } catch (_) {
          /* ignore */
        }
      }
      return;
    }
    if (
      finite(nextNow.seq, 0) !== trackSeq ||
      finite(nextNow.audio_rev, 0) !== audioRev
    ) {
      // The page will call start() again for the new track/audio revision.
      active = false;
      stopSource();
      resetMedia();
      currentStream = null;
      accepted = freshAccepted();
      updateMode(true);
      return;
    }
    if (sourceRole() && wanted && !sourcePump) startSource();
    if (!nextNow.paused && wanted && !userPaused && audioEl.paused && media && media.started) {
      // applyNow() may have paused us; resume once we have media.
      audioEl.play().catch(() => {});
    }
  }

  function setPaused(value) {
    userPaused = Boolean(value);
  }

  function setWanted(value) {
    wanted = Boolean(value);
    if (!wanted) {
      // A paused seed still serves its children; only stop reading the
      // upstream when nothing downstream depends on this device.
      if (!(role === "seed" && downstreams.size > 0)) stopSource();
    } else if (active && sourceRole() && !sourcePump) {
      startSource();
    }
  }

  function matches(nextNow) {
    return Boolean(
      active &&
        nextNow &&
        nextNow.track &&
        finite(nextNow.seq, 0) === trackSeq &&
        finite(nextNow.audio_rev, 0) === audioRev
    );
  }

  function positionMs() {
    const position = mediaPositionMs();
    if (position === null) {
      return nowSnapshot ? finite(nowSnapshot.position_ms, 0) : 0;
    }
    return Math.max(0, position);
  }

  function seekLive() {
    if (!currentStream || !media || !media.started) return;
    const target = currentStream.firstStartMs + (serverNow() - currentStream.firstServerMs);
    const targetSeconds = (target - currentStream.firstStartMs) / 1000;
    let end = null;
    try {
      if (media.buffer && media.buffer.buffered.length) {
        end = media.buffer.buffered.end(media.buffer.buffered.length - 1);
      }
    } catch (_) {
      end = null;
    }
    if (end !== null) {
      // The paused reader kept fetching chunks, but the source ring/buffer
      // may not contain the whole wall-clock gap. If the live target is
      // already past the buffered edge, restart the media timeline at the
      // newest chunk instead of replaying several seconds behind forever.
      if (
        targetSeconds > end + 1.0 &&
        Date.now() - lastResyncAt > 4_000
      ) {
        lastResyncAt = Date.now();
        if (sourceRole()) {
          // The source node is already at the live producer edge. Rebuilding
          // its MSE here causes the audible 0 -> live reset loop; let the
          // source pump fill the buffer instead.
          return;
        }
        const mime = currentStream ? currentStream.mime : "audio/mpeg";
        accepted = freshAccepted();
        currentStream = null;
        if (!setupMedia(mime)) failMesh("mse");
        return;
      }
      const max = Math.max(0, end - 0.25);
      if (targetSeconds >= 0 && targetSeconds <= max) {
        if (
          Math.abs(audioEl.currentTime - targetSeconds) >
          SYNC_TOLERANCE_MS / 1000
        ) {
          try {
            audioEl.currentTime = targetSeconds;
          } catch (_) {
            /* ignore */
          }
        }
      }
    }
  }

  function statusText() {
    if (!active) return "";
    return lastMode || "P2P 连接中";
  }

  function stats() {
    return {
      active,
      role,
      nodeId,
      preferredRole,
      canRelay,
      trackSeq,
      audioRev,
      streamId: accepted.streamId,
      firstSeq: accepted.firstSeq,
      lastSeq: accepted.lastSeq,
      chunks: accepted.chunks,
      dup: accepted.dup,
      gaps: accepted.gaps,
      bad: accepted.bad,
      bytes: accepted.bytes,
      parents: Array.from(upstreams.values())
        .filter((item) => item.open)
        .map((item) => ({ id: item.id, chunks: item.chunks, lastSeq: item.lastSeq })),
      children: Array.from(downstreams.values())
        .filter((item) => item.open)
        .map((item) => ({ id: item.id, queued: item.queue.length })),
      bufferMs: Math.round(bufferAheadMs()),
      positionMs: Math.round(positionMs()),
      clockOffset,
      source: sourcePump
        ? { streamId: sourcePump.manifest && sourcePump.manifest.stream_id, nextSeq: sourcePump.nextSeq }
        : null,
      trackMode: TRACK_MODE,
      audioParent: activeAudioParent,
      audioTrack: localAudioTrack ? localAudioTrack.readyState : null,
      fallbackReason,
      lastError,
    };
  }

  function setFallbackCallback(callback) {
    fallbackCallback = typeof callback === "function" ? callback : null;
  }

  function setLiveCallback(callback) {
    liveCallback = typeof callback === "function" ? callback : null;
    if (liveCallback && activeAudioParent) markLive();
  }

  function shutdown() {
    stopSource();
    resetMedia();
    active = false;
    leaveMesh();
  }

  if (TRACK_MODE) {
    audioEl.addEventListener("playing", () => {
      if (audioContext && audioContext.state === "suspended") {
        audioContext.resume().catch(() => {});
      }
      ensureLocalAudioTrack();
      markLive();
    });
  }

  window.addEventListener("beforeunload", () => {
    try {
      send({ type: "bye" });
    } catch (_) {
      /* ignore */
    }
  });

  setInterval(syncTick, 1200);
  setInterval(updateMode, 2500);

  window.RadioMesh = {
    start,
    stop,
    update,
    setWanted,
    setPaused,
    noteStall,
    matches,
    positionMs,
    seekLive,
    statusText,
    stats,
    fail: failMesh,
    shutdown,
    setFallbackCallback,
    setLiveCallback,
    isActive: () => active,
    isSupported: mseSupported,
  };
})();
