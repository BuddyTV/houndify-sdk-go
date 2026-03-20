package houndify_test

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	houndify "github.com/soundhound/houndify-sdk-go"
	"gotest.tools/assert"
)

func assertChanClosedWithinTimeout(t *testing.T, ch <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("DEADLOCK: %s", msg)
	}
}

func testPartials() []string {
	return []string{
		buildPartialTranscriptLine("", false, false),
		buildPartialTranscriptLine("", false, false),
		buildPartialTranscriptLine("", false, false),
		buildPartialTranscriptLine("", false, false),
		buildPartialTranscriptLine("you", false, false),
		buildPartialTranscriptLine("you", false, false),
		buildPartialTranscriptLine("you too", false, false),
		buildPartialTranscriptLine("you too", false, false),
		buildPartialTranscriptLine("youtube", false, false),
		buildPartialTranscriptLine("youtube", true, false), // SafeToStopAudio=true
	}
}

// TestVoiceSearch_SafeToStopAudioAbort ensures that the SDK
// aborts the audio stream immediately after SafeToStopAudio=true.
func TestVoiceSearch_SafeToStopAudioAbort(t *testing.T) {
	// Infinite slow audio stream — without abort, this never stops.
	audio := &slowReader{delay: 20 * time.Millisecond}

	// respPR/respPW: mock server writes response lines, SDK reads them via resp.Body.
	respPR, respPW := io.Pipe()

	mockTransport := RoundTripFunc(func(req *http.Request) *http.Response {
		go func() {
			// Drain the request body concurrently while streaming partial transcripts, simulating
			// real Houndify server.
			audioDrained := make(chan struct{})
			go func() {
				// When abortableReader.Abort() is called, Read() should return EOF
				// and io.Copy returns here, closing audioDrained.
				io.Copy(io.Discard, req.Body)
				close(audioDrained)
			}()

			// Stream production partial transcripts to the SDK.
			for _, p := range testPartials() {
				fmt.Fprintf(respPW, "%s\n", p)
				time.Sleep(10 * time.Millisecond)
			}

			// Wait for the SDK to stop sending audio after SafeToStopAudio.
			select {
			case <-audioDrained:
				// Abort worked: send the final response, just like a real server.
				fmt.Fprintf(respPW, "%s\n", buildFinalResponseLine())
				respPW.Close()

			case <-time.After(3 * time.Second):
				// Audio is still flowing — the SDK failed to abort.
				// Simulate the server ending the connection.
				respPW.CloseWithError(fmt.Errorf("read tcp: connection reset by peer"))
			}
		}()

		return &http.Response{
			StatusCode: 200,
			Body:       respPR,
			Header:     make(http.Header),
		}
	})

	client := houndify.Client{
		ClientID:   "test-client-id",
		ClientKey:  "vHSRCJhQa6cIzZ6hCrQHwcKDQbdyBuV6mqFXuBG9vAQe3MqjVIEheNDoaTP6n-DQSzhoBsOJwOP5IrWM2pF1fg==",
		HttpClient: &http.Client{Transport: mockTransport},
	}

	voiceReq := houndify.VoiceRequest{
		AudioStream: audio,
		UserID:      "test-user",
		RequestID:   "test-request",
		RequestInfoFields: map[string]interface{}{
			"ServerDeterminesEndOfAudio": true,
		},
	}

	partialChan := make(chan houndify.PartialTranscript, 20)
	chanClosed := make(chan struct{})
	var received []houndify.PartialTranscript
	go func() {
		for p := range partialChan {
			received = append(received, p)
		}
		close(chanClosed)
	}()

	result, err := client.VoiceSearch(voiceReq, partialChan)

	assert.NilError(t, err)
	assert.Assert(t, strings.Contains(result, "SoundHoundVoiceSearchResult"), "expected SoundHoundVoiceSearchResult in response, got: %s", result)

	// Partial transcript channel must close
	assertChanClosedWithinTimeout(t, chanClosed,
		"partial channel not closed after successful VoiceSearch")
	t.Logf("received %d partials, channel closed correctly", len(received))
}

// TestVoiceSearch_ChannelClosedOnError verifies that the partial transcript
// channel is closed even when VoiceSearch returns an error.
func TestVoiceSearch_ChannelClosedOnError(t *testing.T) {
	audio := &slowReader{delay: 20 * time.Millisecond}

	respPR, respPW := io.Pipe()

	mockTransport := RoundTripFunc(func(req *http.Request) *http.Response {
		go func() {
			go io.Copy(io.Discard, req.Body)

			// Stream some partials, then RST before sending the final response.
			for _, p := range testPartials() {
				fmt.Fprintf(respPW, "%s\n", p)
				time.Sleep(10 * time.Millisecond)
			}
			respPW.CloseWithError(fmt.Errorf("read tcp: connection reset by peer: test error"))
		}()

		return &http.Response{
			StatusCode: 200,
			Body:       respPR,
			Header:     make(http.Header),
		}
	})

	client := houndify.Client{
		ClientID:   "test-client-id",
		ClientKey:  "vHSRCJhQa6cIzZ6hCrQHwcKDQbdyBuV6mqFXuBG9vAQe3MqjVIEheNDoaTP6n-DQSzhoBsOJwOP5IrWM2pF1fg==",
		HttpClient: &http.Client{Transport: mockTransport},
	}

	voiceReq := houndify.VoiceRequest{
		AudioStream: audio,
		UserID:      "test-user",
		RequestID:   "test-request",
		RequestInfoFields: map[string]interface{}{
			"ServerDeterminesEndOfAudio": true,
		},
	}

	partialChan := make(chan houndify.PartialTranscript, 20)
	chanClosed := make(chan struct{})
	go func() {
		for range partialChan {
		}
		close(chanClosed)
	}()

	_, err := client.VoiceSearch(voiceReq, partialChan)

	assert.ErrorContains(t, err, "error reading Houndify server response")
	assertChanClosedWithinTimeout(t, chanClosed, "partial channel not closed after VoiceSearch read error")
}

// TestVoiceSearch_BuildRequestError verifies that VoiceSearch returns an error
// and closes the partial transcript channel when BuildRequest fails.
// BuildRequest fails when the ClientKey is not valid base64.
func TestVoiceSearch_BuildRequestError(t *testing.T) {
	partialChan := make(chan houndify.PartialTranscript, 10)
	chanClosed := make(chan struct{})
	go func() {
		for range partialChan {
		}
		close(chanClosed)
	}()

	client := houndify.Client{
		ClientID:  "test-client-id",
		ClientKey: "!!!not-valid-base64!!!",
	}

	voiceReq := houndify.VoiceRequest{
		AudioStream:       strings.NewReader("audio"),
		UserID:            "test-user",
		RequestID:         "test-request",
		RequestInfoFields: map[string]interface{}{},
	}

	_, err := client.VoiceSearch(voiceReq, partialChan)

	assert.ErrorContains(t, err, "failed to decode client key")
	assertChanClosedWithinTimeout(t, chanClosed,
		"partial channel not closed after BuildRequest failure")
}

// TestVoiceSearch_HttpClientDoError verifies that VoiceSearch returns an error
// and closes the partial transcript channel when the HTTP transport fails.
func TestVoiceSearch_HttpClientDoError(t *testing.T) {
	partialChan := make(chan houndify.PartialTranscript, 10)
	chanClosed := make(chan struct{})
	go func() {
		for range partialChan {
		}
		close(chanClosed)
	}()

	mockTransport := RoundTripFunc(func(req *http.Request) *http.Response {
		// Return nil to force an error from http.Client.Do.
		return nil
	})

	client := houndify.Client{
		ClientID:   "test-client-id",
		ClientKey:  "vHSRCJhQa6cIzZ6hCrQHwcKDQbdyBuV6mqFXuBG9vAQe3MqjVIEheNDoaTP6n-DQSzhoBsOJwOP5IrWM2pF1fg==",
		HttpClient: &http.Client{Transport: mockTransport},
	}

	voiceReq := houndify.VoiceRequest{
		AudioStream:       strings.NewReader("audio"),
		UserID:            "test-user",
		RequestID:         "test-request",
		RequestInfoFields: map[string]interface{}{},
	}

	_, err := client.VoiceSearch(voiceReq, partialChan)

	assert.ErrorContains(t, err, "failed to successfully run request")
	assertChanClosedWithinTimeout(t, chanClosed,
		"partial channel not closed after HttpClient.Do failure")
}
