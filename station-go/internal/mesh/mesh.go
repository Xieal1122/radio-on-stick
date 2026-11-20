package mesh

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	guid         = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
	maxChildren  = 4
	maxParents   = 2
	minParents   = 2
	maxSeeds     = 2
	maxMessage   = 256 * 1024
	heartbeatSec = 10.0
	idleSec      = 30.0
)

type conn struct {
	socket    net.Conn
	reader    *bufio.Reader
	writeMu   sync.Mutex
	closed    bool
	lastRecv  time.Time
	lastPing  time.Time
	fragments [][]byte
	fragOp    byte
}

func newConn(socket net.Conn, initial *bufio.Reader) *conn {
	now := time.Now()
	return &conn{socket: socket, reader: initial, lastRecv: now, lastPing: now}
}

func (c *conn) sendFrame(opcode byte, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.closed {
		return io.ErrClosedPipe
	}
	header := []byte{0x80 | opcode}
	length := len(payload)
	switch {
	case length < 126:
		header = append(header, byte(length))
	case length < 65536:
		header = append(header, 126, byte(length>>8), byte(length))
	default:
		header = append(header, 127)
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(length))
		header = append(header, size[:]...)
	}
	_ = c.socket.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := c.socket.Write(append(header, payload...))
	if err != nil {
		c.closed = true
	}
	return err
}

func (c *conn) sendJSON(value any) error {
	payload, _ := json.Marshal(value)
	return c.sendFrame(0x1, payload)
}

func (c *conn) close() {
	if c.closed {
		return
	}
	c.closed = true
	_ = c.sendFrame(0x8, nil)
	_ = c.socket.Close()
}

func (c *conn) readMessage() ([]byte, error) {
	for {
		first, err := c.reader.ReadByte()
		if err != nil {
			return nil, err
		}
		second, err := c.reader.ReadByte()
		if err != nil {
			return nil, err
		}
		fin := first&0x80 != 0
		opcode := first & 0x0f
		masked := second&0x80 != 0
		length := int(second & 0x7f)
		if length == 126 {
			var size [2]byte
			if _, err := io.ReadFull(c.reader, size[:]); err != nil {
				return nil, err
			}
			length = int(binary.BigEndian.Uint16(size[:]))
		} else if length == 127 {
			var size [8]byte
			if _, err := io.ReadFull(c.reader, size[:]); err != nil {
				return nil, err
			}
			length = int(binary.BigEndian.Uint64(size[:]))
		}
		if length > maxMessage {
			return nil, fmt.Errorf("websocket message too large")
		}
		var mask [4]byte
		if masked {
			if _, err := io.ReadFull(c.reader, mask[:]); err != nil {
				return nil, err
			}
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(c.reader, payload); err != nil {
			return nil, err
		}
		if masked {
			for i := range payload {
				payload[i] ^= mask[i%4]
			}
		}
		switch opcode {
		case 0x8:
			return nil, io.EOF
		case 0x9:
			_ = c.sendFrame(0xA, payload)
			continue
		case 0xA:
			continue
		case 0x1, 0x2, 0x0:
			if opcode == 0x0 {
				if len(c.fragments) == 0 {
					return nil, fmt.Errorf("unexpected continuation")
				}
				c.fragments = append(c.fragments, payload)
				if !fin {
					continue
				}
				payload = []byte{}
				for _, part := range c.fragments {
					payload = append(payload, part...)
				}
				c.fragments = nil
				return payload, nil
			}
			if !fin {
				c.fragOp = opcode
				c.fragments = [][]byte{payload}
				continue
			}
			return payload, nil
		}
	}
}

type Node struct {
	ID            string
	Conn          *conn
	Local         bool
	PreferredRole string
	CanRelay      bool
	Role          string
	Parents       map[string]bool
	Children      map[string]bool
	JoinedAt      time.Time
	Hello         bool
	Waiting       bool
	Blocked       map[string]time.Time
}

func newNode(id string, socket *conn, local bool) *Node {
	return &Node{
		ID: id, Conn: socket, Local: local, PreferredRole: "child", CanRelay: true,
		Role: "child", Parents: map[string]bool{}, Children: map[string]bool{},
		JoinedAt: time.Now(), Blocked: map[string]time.Time{},
	}
}

type Hub struct {
	mu            sync.Mutex
	nodes         map[string]*Node
	seeds         map[string]bool
	nextNumber    int
	closed        bool
	localSeedOnly bool
}

func NewHub() *Hub {
	return &Hub{nodes: map[string]*Node{}, seeds: map[string]bool{}}
}

// SetLocalSeedOnly prevents a browser reached through a relay from becoming
// an audio seed. Only a browser connected directly from the station host may
// fetch /source and relay it to remote listeners.
func (h *Hub) SetLocalSeedOnly(value bool) {
	h.mu.Lock()
	h.localSeedOnly = value
	h.mu.Unlock()
}

func (h *Hub) isLocalSeedOnly() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.localSeedOnly
}

// In p2p-only mode the station host seed is the only HTTP source. Requiring a
// second upstream before a listener can relay would make the first listener a
// leaf forever, collapsing the whole audience onto one seed connection.
// One healthy upstream is enough to start relaying; a second is added when
// available for resilience.
func (h *Hub) requiredParents() int {
	if h.localSeedOnly {
		return 1
	}
	return minParents
}

func localRequest(r *http.Request) bool {
	if r.Header.Get("X-Forwarded-For") != "" || r.Header.Get("X-Real-IP") != "" {
		return false
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (h *Hub) send(node *Node, value any) bool {
	if err := node.Conn.sendJSON(value); err != nil {
		node.Conn.closed = true
		return false
	}
	return true
}

func (h *Hub) sendError(node *Node, code, message string) {
	h.send(node, map[string]any{"type": "error", "code": code, "message": message})
}

func (h *Hub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		http.Error(w, "websocket required", http.StatusBadRequest)
		return
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	if key == "" {
		http.Error(w, "bad websocket", http.StatusBadRequest)
		return
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "unsupported", http.StatusInternalServerError)
		return
	}
	socket, buffered, err := hijacker.Hijack()
	if err != nil {
		return
	}
	accept := sha1.Sum([]byte(key + guid))
	_, _ = fmt.Fprintf(buffered, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", base64.StdEncoding.EncodeToString(accept[:]))
	_ = buffered.Flush()
	node := newNode("", newConn(socket, buffered.Reader), localRequest(r))
	h.serve(node)
}

func (h *Hub) serve(node *Node) {
	defer h.cleanup(node)
	_ = node.Conn.socket.SetReadDeadline(time.Now().Add(time.Second))
	for {
		h.mu.Lock()
		closed := h.closed
		h.mu.Unlock()
		if closed || node.Conn.closed {
			return
		}
		now := time.Now()
		if node.Hello && now.Sub(node.Conn.lastRecv) > idleSec*time.Second {
			return
		}
		if !node.Hello && now.Sub(node.JoinedAt) > 15*time.Second {
			return
		}
		if now.Sub(node.Conn.lastPing) > heartbeatSec*time.Second {
			_ = node.Conn.sendFrame(0x9, []byte("r"))
			node.Conn.lastPing = now
		}
		_ = node.Conn.socket.SetReadDeadline(time.Now().Add(time.Second))
		payload, err := node.Conn.readMessage()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			return
		}
		node.Conn.lastRecv = time.Now()
		var message map[string]any
		if json.Unmarshal(payload, &message) != nil {
			h.mu.Lock()
			h.sendError(node, "bad_json", "消息不是合法 JSON")
			h.mu.Unlock()
			continue
		}
		h.handle(node, message)
	}
}

func (h *Hub) targetSeedCount() int {
	if h.localSeedOnly {
		for _, node := range h.nodes {
			if node.Local && node.Hello && node.CanRelay {
				return 1
			}
		}
		return 0
	}
	if len(h.nodes) >= 2 {
		return maxSeeds
	}
	if len(h.nodes) >= 1 {
		return 1
	}
	return 0
}

func (h *Hub) chooseSeed() *Node {
	var candidates []*Node
	for _, node := range h.nodes {
		if node.Role == "child" && node.Hello && node.CanRelay && (!h.localSeedOnly || node.Local) {
			candidates = append(candidates, node)
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	best := candidates[0]
	for _, node := range candidates[1:] {
		if node.PreferredRole == "seed" && best.PreferredRole != "seed" {
			best = node
		} else if len(node.Children) < len(best.Children) {
			best = node
		}
	}
	return best
}

func (h *Hub) promote(node *Node) {
	if node.Role == "seed" || !node.CanRelay || (h.localSeedOnly && !node.Local) {
		return
	}
	for parentID := range node.Parents {
		h.detach(parentID, node.ID, "promoted", true, false)
	}
	node.Parents = map[string]bool{}
	node.Waiting = false
	node.Blocked = map[string]time.Time{}
	node.Role = "seed"
	h.seeds[node.ID] = true
	h.send(node, map[string]any{"type": "role", "role": "seed", "source": map[string]any{"manifest_url": "/source/manifest", "chunk_url": "/source/chunk"}})
}

func (h *Hub) maintainSeeds() {
	target := h.targetSeedCount()
	for guard := 0; len(h.seeds) < target && guard < 10; guard++ {
		candidate := h.chooseSeed()
		if candidate == nil {
			break
		}
		h.promote(candidate)
	}
}

func (h *Hub) isLinked(a, b *Node) bool {
	return a.Parents[b.ID] || a.Children[b.ID] || b.Parents[a.ID] || b.Children[a.ID]
}

func (h *Hub) isAncestor(ancestorID, nodeID string) bool {
	seen := map[string]bool{}
	stack := []string{nodeID}
	for len(stack) > 0 {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[id] {
			continue
		}
		seen[id] = true
		node := h.nodes[id]
		if node == nil {
			continue
		}
		for parentID := range node.Parents {
			if parentID == ancestorID {
				return true
			}
			stack = append(stack, parentID)
		}
	}
	return false
}

func (h *Hub) candidatesFor(child *Node) []*Node {
	var result []*Node
	seen := map[string]bool{}
	queue := []string{}
	for id := range h.seeds {
		queue = append(queue, id)
	}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if seen[id] {
			continue
		}
		seen[id] = true
		candidate := h.nodes[id]
		if candidate == nil || candidate.ID == child.ID {
			continue
		}
		for childID := range candidate.Children {
			queue = append(queue, childID)
		}
		if !candidate.CanRelay || candidate.ID == child.ID {
			continue
		}
		if candidate.Role == "child" && len(candidate.Parents) < h.requiredParents() {
			continue
		}
		if child.Parents[candidate.ID] || len(candidate.Children) >= maxChildren {
			continue
		}
		if deadline, ok := child.Blocked[candidate.ID]; ok && deadline.After(time.Now()) {
			continue
		}
		if h.isAncestor(child.ID, candidate.ID) {
			continue
		}
		result = append(result, candidate)
	}
	return result
}

func (h *Hub) addParent(child, parent *Node, reason string) bool {
	if !child.Hello || !parent.Hello || child.Role == "seed" || child.ID == parent.ID {
		return false
	}
	if len(child.Parents) >= maxParents || len(parent.Children) >= maxChildren || !parent.CanRelay || child.Parents[parent.ID] {
		return false
	}
	if h.isAncestor(child.ID, parent.ID) {
		return false
	}
	if deadline, ok := child.Blocked[parent.ID]; ok && deadline.After(time.Now()) {
		return false
	}
	child.Parents[parent.ID] = true
	parent.Children[child.ID] = true
	child.Waiting = false
	h.send(child, map[string]any{"type": "parent-added", "parent_id": parent.ID, "reason": reason})
	h.send(parent, map[string]any{"type": "child-joined", "child_id": child.ID, "reason": reason})
	return true
}

func (h *Hub) detach(parentID, childID, reason string, notifyParent, notifyChild bool) bool {
	parent, child := h.nodes[parentID], h.nodes[childID]
	if parent == nil || child == nil || !parent.Children[childID] {
		return false
	}
	delete(parent.Children, childID)
	delete(child.Parents, parentID)
	if notifyParent {
		h.send(parent, map[string]any{"type": "child-left", "child_id": childID, "reason": reason})
	}
	if notifyChild {
		h.send(child, map[string]any{"type": "parent-removed", "parent_id": parentID, "reason": reason})
	}
	return true
}

func (h *Hub) ensureParents(child *Node, reason string) {
	if !child.Hello || child.Role == "seed" || len(child.Parents) >= maxParents {
		return
	}
	for _, candidate := range h.candidatesFor(child) {
		if len(child.Parents) >= maxParents {
			break
		}
		h.addParent(child, candidate, reason)
	}
	if len(child.Parents) < h.requiredParents() {
		if !child.Waiting {
			child.Waiting = true
			code := "need_second_parent"
			if len(child.Parents) == 0 {
				code = "no_capacity"
			}
			h.send(child, map[string]any{"type": "waiting", "reason": code})
		}
	} else {
		child.Waiting = false
	}
}

func (h *Hub) topUpAll(reason string) {
	for _, child := range h.nodes {
		if child.Role == "child" && child.Hello && len(child.Parents) < maxParents {
			h.ensureParents(child, reason)
		}
	}
}

func (h *Hub) handle(node *Node, message map[string]any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	kind, _ := message["type"].(string)
	if kind == "hello" {
		h.hello(node, message)
		return
	}
	if !node.Hello {
		h.sendError(node, "hello_required", "请先发送 hello")
		return
	}
	switch kind {
	case "signal":
		h.signal(node, message)
	case "upstream-lost":
		parentID, _ := message["parent_id"].(string)
		if parentID != "" && node.Parents[parentID] {
			h.detach(parentID, node.ID, "upstream-lost", true, false)
			node.Blocked[parentID] = time.Now().Add(8 * time.Second)
			node.Waiting = false
			h.ensureParents(node, "upstream-lost")
			h.topUpAll("upstream-lost")
		}
	case "downstream-lost":
		childID, _ := message["child_id"].(string)
		if childID != "" && node.Children[childID] {
			if child := h.nodes[childID]; child != nil {
				h.detach(node.ID, childID, "downstream-lost", false, true)
				child.Blocked[node.ID] = time.Now().Add(8 * time.Second)
				child.Waiting = false
				h.ensureParents(child, "downstream-lost")
				h.topUpAll("downstream-lost")
			}
		}
	case "bye":
		h.cleanupLocked(node)
	case "ping":
		h.send(node, map[string]any{"type": "pong", "ts": time.Now().UnixMilli()})
	}
}

func (h *Hub) hello(node *Node, message map[string]any) {
	if node.Hello {
		h.sendError(node, "already_hello", "重复 hello")
		return
	}
	h.nextNumber++
	node.ID = fmt.Sprintf("n%d", h.nextNumber)
	rawPreferred, _ := message["preferred_role"].(string)
	rawPreferred = strings.ToLower(rawPreferred)
	canRelay := rawPreferred != "leaf"
	if value, ok := message["can_relay"].(bool); ok && !value {
		canRelay = false
	}
	preferred := "child"
	if rawPreferred == "seed" && canRelay && (!h.localSeedOnly || node.Local) {
		preferred = "seed"
	}
	node.PreferredRole = preferred
	node.CanRelay = canRelay
	node.Hello = true
	node.JoinedAt = time.Now()
	h.nodes[node.ID] = node
	target := h.targetSeedCount()
	role := "child"
	if !canRelay {
		role = "child"
	} else if !h.localSeedOnly || node.Local {
		if len(h.seeds) == 0 || len(h.seeds) < target {
			role = "seed"
		}
	}
	node.Role = role
	if role == "seed" {
		h.seeds[node.ID] = true
	}
	h.send(node, map[string]any{
		"type": "welcome", "id": node.ID, "role": role, "preferred_role": preferred,
		"capacity": maxChildren, "max_parents": maxParents, "min_parents": h.requiredParents(),
		"can_relay": canRelay,
		"source": map[string]any{"manifest_url": "/source/manifest", "chunk_url": "/source/chunk"},
	})
	h.maintainSeeds()
	h.topUpAll("join")
}

func (h *Hub) signal(node *Node, message map[string]any) {
	targetID, _ := message["to"].(string)
	payload, ok := message["payload"].(map[string]any)
	if targetID == "" || !ok {
		h.sendError(node, "bad_signal", "信令缺少 to 或 payload")
		return
	}
	target := h.nodes[targetID]
	if target == nil || !target.Hello {
		h.sendError(node, "not_found", "目标节点不存在")
		return
	}
	if !h.isLinked(node, target) {
		h.sendError(node, "not_neighbor", "只能和直接父/子节点交换信令")
		return
	}
	kind, _ := payload["kind"].(string)
	if kind != "offer" && kind != "answer" && kind != "ice" {
		h.sendError(node, "bad_signal", "未知信令类型")
		return
	}
	h.send(target, map[string]any{"type": "signal", "from": node.ID, "payload": payload})
}

func (h *Hub) cleanup(node *Node) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cleanupLocked(node)
}

func (h *Hub) cleanupLocked(node *Node) {
	if _, exists := h.nodes[node.ID]; !exists {
		node.Conn.close()
		return
	}
	for childID := range node.Children {
		h.detach(node.ID, childID, "parent-left", false, true)
	}
	for parentID := range node.Parents {
		h.detach(parentID, node.ID, "child-left", true, false)
	}
	delete(h.nodes, node.ID)
	delete(h.seeds, node.ID)
	node.Conn.close()
	h.maintainSeeds()
	h.topUpAll("leave")
}

func (h *Hub) Status() map[string]any {
	h.mu.Lock()
	defer h.mu.Unlock()
	seeds := make([]string, 0, len(h.seeds))
	for id := range h.seeds {
		seeds = append(seeds, id)
	}
	nodes := make([]map[string]any, 0, len(h.nodes))
	for _, node := range h.nodes {
		parents := make([]string, 0, len(node.Parents))
		for id := range node.Parents {
			parents = append(parents, id)
		}
		children := make([]string, 0, len(node.Children))
		for id := range node.Children {
			children = append(children, id)
		}
		nodes = append(nodes, map[string]any{
			"id": node.ID, "role": node.Role, "parents": parents,
			"children": children, "preferred": node.PreferredRole,
			"can_relay": node.CanRelay, "waiting": node.Waiting,
		})
	}
	return map[string]any{"seeds": seeds, "nodes": nodes}
}
