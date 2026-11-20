// Package rtcseed turns the station's current audio stream into a WebRTC
// Opus track. It is the server-side seed: no headless listener browser is
// needed, and the VPS is never in the media path.
package rtcseed

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"

	"radio-on-stick/station-go/internal/library"
)

const opusClockRate = 48000
const opusFrameSamples = 960 // 20 ms

type Source struct {
	mu       sync.Mutex
	api      *webrtc.API
	track    *webrtc.TrackLocalStaticRTP
	stun     string
	key      string
	cancel   context.CancelFunc
	conn     *net.UDPConn
	cmd      *exec.Cmd
	peers    map[*webrtc.PeerConnection]struct{}
	seq      uint16
	timestamp uint32
	stopOnce sync.Once
}

func New(stun string) (*Source, error) {
	media := &webrtc.MediaEngine{}
	if err := media.RegisterDefaultCodecs(); err != nil {
		return nil, err
	}
	api := webrtc.NewAPI(webrtc.WithMediaEngine(media))
	track, err := webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{
		MimeType:  webrtc.MimeTypeOpus,
		ClockRate: opusClockRate,
		Channels:  2,
	}, "audio", "radio")
	if err != nil {
		return nil, err
	}
	return &Source{
		api: api, track: track, stun: strings.TrimSpace(stun),
		peers: map[*webrtc.PeerConnection]struct{}{},
		seq: uint16(rand.Uint32()), timestamp: rand.Uint32(),
	}, nil
}

// Ensure starts an Opus RTP producer for track when the track changes.
func (s *Source) Ensure(track library.Track, positionMS int64) {
	input := strings.TrimSpace(track.Path)
	if input == "" {
		input = strings.TrimSpace(track.URL)
	}
	if input == "" || track.ID == "" {
		s.Stop()
		return
	}
	s.mu.Lock()
	if s.key == track.ID {
		s.mu.Unlock()
		return
	}
	s.stopLocked()
	s.key = track.ID
	s.mu.Unlock()
	if err := s.start(track, input, positionMS); err != nil {
		log.Printf("rtc seed %s: %v", track.ID, err)
	}
}

func (s *Source) start(track library.Track, input string, positionMS int64) error {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		return err
	}
	port := conn.LocalAddr().(*net.UDPAddr).Port
	args := []string{
		"-hide_banner", "-loglevel", "error", "-nostdin", "-re",
	}
	if positionMS > 0 {
		args = append(args, "-ss", fmt.Sprintf("%.3f", float64(positionMS)/1000))
	}
	args = append(args,
		"-i", input, "-vn",
		"-c:a", "libopus", "-b:a", "64k", "-ar", "48000", "-ac", "2",
		"-frame_duration", "20",
		"-f", "rtp", "-payload_type", "111",
		fmt.Sprintf("rtp://127.0.0.1:%d?pkt_size=1200", port),
	)
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	// Keep the encoder in its own process group so station shutdown cannot
	// leave an orphan ffmpeg writing to a closed RTP socket.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		cancel()
		_ = conn.Close()
		return err
	}
	s.mu.Lock()
	s.conn = conn
	s.cmd = cmd
	s.cancel = cancel
	s.mu.Unlock()
	go s.readLoop(conn, ctx)
	return nil
}

func (s *Source) readLoop(conn *net.UDPConn, ctx context.Context) {
	defer conn.Close()
	buf := make([]byte, 2048)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return
		}
		var packet rtp.Packet
		if packet.Unmarshal(buf[:n]) != nil || len(packet.Payload) == 0 {
			continue
		}
		s.mu.Lock()
		seq := s.seq
		timestamp := s.timestamp
		s.seq++
		s.timestamp += opusFrameSamples
		s.mu.Unlock()
		packet.Header.SequenceNumber = seq
		packet.Header.Timestamp = timestamp
		packet.Header.SSRC = 1
		packet.Header.PayloadType = 111
		_ = s.track.WriteRTP(&packet)
	}
}

func (s *Source) Answer(offerSDP string) (string, error) {
	answer, err := s.Offer(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer, SDP: offerSDP,
	})
	if err != nil {
		return "", err
	}
	return answer.SDP, nil
}

// Offer answers one browser offer and sends the shared RTP track.
func (s *Source) Offer(offer webrtc.SessionDescription) (webrtc.SessionDescription, error) {
	config := webrtc.Configuration{}
	if s.stun != "" {
		config.ICEServers = []webrtc.ICEServer{{URLs: []string{s.stun}}}
	}
	pc, err := s.api.NewPeerConnection(config)
	if err != nil {
		return webrtc.SessionDescription{}, err
	}
	if _, err := pc.AddTrack(s.track); err != nil {
		_ = pc.Close()
		return webrtc.SessionDescription{}, err
	}
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateClosed {
			_ = pc.Close()
		}
	})
	s.mu.Lock()
	s.peers[pc] = struct{}{}
	s.mu.Unlock()
	if err := pc.SetRemoteDescription(offer); err != nil {
		_ = pc.Close()
		return webrtc.SessionDescription{}, err
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		_ = pc.Close()
		return webrtc.SessionDescription{}, err
	}
	gather := webrtc.GatheringCompletePromise(pc)
	if err := pc.SetLocalDescription(answer); err != nil {
		_ = pc.Close()
		return webrtc.SessionDescription{}, err
	}
	select {
	case <-gather:
	case <-time.After(8 * time.Second):
	}
	if local := pc.LocalDescription(); local != nil {
		return *local, nil
	}
	_ = pc.Close()
	return webrtc.SessionDescription{}, fmt.Errorf("no local description")
}

func (s *Source) Stop() {
	s.mu.Lock()
	s.stopLocked()
	s.mu.Unlock()
}

func (s *Source) stopLocked() {
	s.key = ""
	if s.cmd != nil && s.cmd.Process != nil {
		_ = syscall.Kill(-s.cmd.Process.Pid, syscall.SIGKILL)
	}
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
	s.cmd = nil
	for pc := range s.peers {
		_ = pc.Close()
		delete(s.peers, pc)
	}
}

func (s *Source) Close() { s.Stop() }
