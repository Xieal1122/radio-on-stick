package stream

import "testing"

func TestParseMP3FrameHeader(t *testing.T) {
	header := []byte{0xFF, 0xFB, 0x90, 0xC4, 0, 0, 0, 0}
	frame := parseMP3Frame(header)
	if frame == nil {
		t.Fatal("header rejected")
	}
	if frame.length != 417 {
		t.Fatalf("length = %d", frame.length)
	}
	if frame.durationMS != 26 {
		t.Fatalf("duration = %d", frame.durationMS)
	}
}

func TestParseMP3RejectsGarbage(t *testing.T) {
	if parseMP3Frame([]byte{0, 1, 2, 3}) != nil {
		t.Fatal("garbage accepted")
	}
}
