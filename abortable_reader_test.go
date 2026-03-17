package houndify_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	houndify "github.com/soundhound/houndify-sdk-go"
	"gotest.tools/assert"
)

// slowReader simulates an audio stream that produces data slowly and can be
// observed for whether it is still being read after abort.
type slowReader struct {
	mu        sync.Mutex
	readCount int
	delay     time.Duration
}

func (s *slowReader) Read(p []byte) (int, error) {
	time.Sleep(s.delay)
	s.mu.Lock()
	s.readCount++
	s.mu.Unlock()
	n := copy(p, []byte{0x00, 0x01, 0x02, 0x03})
	return n, nil
}

func (s *slowReader) getReadCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readCount
}

func buildPartialTranscriptLine(transcript string, safeToStop bool, done bool) string {
	msg := map[string]interface{}{
		"Format":            "HoundVoiceQueryPartialTranscript",
		"FormatVersion":     "1.0",
		"PartialTranscript": transcript,
		"DurationMS":        100,
		"Done":              done,
		"SafeToStopAudio":   safeToStop,
	}
	b, _ := json.Marshal(msg)
	return string(b)
}

func buildFinalResponseLine() string {
	msg := map[string]interface{}{
		"Format":        "SoundHoundVoiceSearchResult",
		"FormatVersion": "1.0",
		"Status":        "OK",
		"NumToReturn":   1.0,
		"AllResults": []interface{}{
			map[string]interface{}{
				"WrittenResponseLong": "test response",
				"ConversationState":   map[string]interface{}{},
			},
		},
	}
	b, _ := json.Marshal(msg)
	return string(b)
}

// TestVoiceSearch_AbortOnSafeToStopAudio verifies that when the server sends
// SafeToStopAudio==true, the SDK stops reading from the audio stream promptly,
// preventing writes to a connection the server is about to close.
//
// Without the abortableReader fix, the HTTP transport would continue reading
// from the audio stream indefinitely, and if the server closed the connection
// the transport's write would hit a RST → "connection reset by peer".
func TestVoiceSearch_AbortOnSafeToStopAudio(t *testing.T) {
	audio := &slowReader{delay: 50 * time.Millisecond}

	// Build a mock response body that sends:
	// 1. A partial transcript with SafeToStopAudio=false
	// 2. A partial transcript with SafeToStopAudio=true
	// 3. The final SoundHoundVoiceSearchResult
	var responseLines []string
	responseLines = append(responseLines, buildPartialTranscriptLine("hello", false, false))
	responseLines = append(responseLines, buildPartialTranscriptLine("hello world", true, false))
	responseLines = append(responseLines, buildFinalResponseLine())
	responseBody := strings.Join(responseLines, "\n") + "\n"

	mockClient := NewTestClient(func(req *http.Request) *http.Response {
		// Drain some of the request body to simulate server reading audio,
		// then return the response (server is done with audio).
		buf := make([]byte, 16)
		req.Body.Read(buf)
		return &http.Response{
			StatusCode: 200,
			Body:       ioutil.NopCloser(bytes.NewBufferString(responseBody)),
			Header:     make(http.Header),
		}
	})

	client := houndify.Client{
		ClientID:   "test-client-id",
		ClientKey:  "vHSRCJhQa6cIzZ6hCrQHwcKDQbdyBuV6mqFXuBG9vAQe3MqjVIEheNDoaTP6n-DQSzhoBsOJwOP5IrWM2pF1fg==",
		HttpClient: mockClient,
	}

	voiceReq := houndify.VoiceRequest{
		AudioStream: audio,
		UserID:      "test-user",
		RequestID:   "test-request",
		URL:         "http://test.com/v1/voice",
		RequestInfoFields: map[string]interface{}{
			"ServerDeterminesEndOfAudio": true,
		},
	}

	partials := make(chan houndify.PartialTranscript, 10)
	var receivedPartials []houndify.PartialTranscript
	partialsDone := make(chan struct{})

	// Collect partials in background
	go func() {
		defer close(partialsDone)
		for p := range partials {
			receivedPartials = append(receivedPartials, p)
		}
	}()

	result, err := client.VoiceSearch(voiceReq, partials)
	<-partialsDone

	assert.NilError(t, err)
	assert.Assert(t, result != "", "expected non-empty response body")

	// Verify we received the SafeToStopAudio partial
	foundSafeToStop := false
	for _, p := range receivedPartials {
		if p.SafeToStopAudio != nil && *p.SafeToStopAudio {
			foundSafeToStop = true
		}
	}
	assert.Assert(t, foundSafeToStop, "expected to receive a partial with SafeToStopAudio=true")

	// After VoiceSearch returns, the audio reader should no longer be read.
	// Record read count, wait, and confirm no further reads occurred.
	countAfterSearch := audio.getReadCount()
	time.Sleep(200 * time.Millisecond)
	countAfterWait := audio.getReadCount()

	assert.Equal(t, countAfterSearch, countAfterWait,
		fmt.Sprintf("audio reader was still being read after VoiceSearch returned: reads went from %d to %d",
			countAfterSearch, countAfterWait))
}

// TestVoiceSearch_NoAbortWithoutServerDeterminesEndOfAudio verifies that when
// ServerDeterminesEndOfAudio is NOT set, the abortableReader does not interfere
// with normal audio streaming behavior.
func TestVoiceSearch_NoAbortWithoutServerDeterminesEndOfAudio(t *testing.T) {
	audioData := bytes.Repeat([]byte{0x00}, 128)
	audio := bytes.NewReader(audioData)

	// Use a finite audio stream that returns EOF naturally
	var responseLines []string
	responseLines = append(responseLines, buildPartialTranscriptLine("hello", false, false))
	responseLines = append(responseLines, buildFinalResponseLine())
	responseBody := strings.Join(responseLines, "\n") + "\n"

	var bodyBytes []byte
	mockClient := NewTestClient(func(req *http.Request) *http.Response {
		bodyBytes, _ = io.ReadAll(req.Body)
		return &http.Response{
			StatusCode: 200,
			Body:       ioutil.NopCloser(bytes.NewBufferString(responseBody)),
			Header:     make(http.Header),
		}
	})

	client := houndify.Client{
		ClientID:   "test-client-id",
		ClientKey:  "vHSRCJhQa6cIzZ6hCrQHwcKDQbdyBuV6mqFXuBG9vAQe3MqjVIEheNDoaTP6n-DQSzhoBsOJwOP5IrWM2pF1fg==",
		HttpClient: mockClient,
	}

	voiceReq := houndify.VoiceRequest{
		AudioStream:       audio,
		UserID:            "test-user",
		RequestID:         "test-request",
		URL:               "http://test.com/v1/voice",
		RequestInfoFields: map[string]interface{}{},
	}

	partials := make(chan houndify.PartialTranscript, 10)
	go func() {
		for range partials {
		}
	}()

	result, err := client.VoiceSearch(voiceReq, partials)
	assert.NilError(t, err)
	assert.Assert(t, result != "", "expected non-empty response body")

	// All audio bytes should have been sent (not aborted early)
	assert.Equal(t, len(bodyBytes), len(audioData),
		"expected all audio data to be sent when ServerDeterminesEndOfAudio is not set")
}
