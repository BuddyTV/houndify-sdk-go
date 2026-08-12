package houndify

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

const houndifyVoiceURL = "https://api.houndify.com:443/v1/audio"
const houndifyTextURL = "https://api.houndify.com:443/v1/text"

// Default user agent set by the SDK
const SDKUserAgent = "Go Houndify SDK"

type (
	// A Client holds the configuration and state, which is used for
	// sending all outgoing Houndify requests and appropriately saving their responses.
	Client struct {
		// The ClientID comes from the Houndify site.
		ClientID string
		// The ClientKey comes from the Houndify site.
		// Keep the key secret.
		ClientKey               string
		enableConversationState bool
		conversationState       interface{}
		// If Verbose is true, all data sent from the server is printed to stdout, unformatted and unparsed.
		// This includes partial transcripts, errors, HTTP headers details (status code, headers, etc.), and final response JSON.
		Verbose           bool
		HttpClient        *http.Client
		RequestInfoInBody bool
	}

	// all of the Hound server JSON messages have these basic fields
	houndServerMessage struct {
		Format  string `json:"Format"`
		Version string `json:"FormatVersion"`
	}
	houndServerPartialTranscript struct {
		houndServerMessage
		PartialTranscript string `json:"PartialTranscript"`
		DurationMS        int64  `json:"DurationMS"`
		Done              bool   `json:"Done"`
		SafeToStopAudio   *bool  `json:"SafeToStopAudio"`
	}
)

// EnableConversationState enables conversation state for future queries
func (c *Client) EnableConversationState() {
	c.enableConversationState = true
}

// DisableConversationState disables conversation state for future queries
func (c *Client) DisableConversationState() {
	c.enableConversationState = false
}

// ClearConversationState removes, or "forgets", the current conversation state
func (c *Client) ClearConversationState() {
	var emptyConvState interface{}
	c.conversationState = emptyConvState
}

// GetConversationState returns the current conversation state, useful for saving
func (c *Client) GetConversationState() interface{} {
	return c.conversationState
}

// SetConversationState sets the conversation state, useful for resuming from a saved point
func (c *Client) SetConversationState(newState interface{}) {
	c.conversationState = newState
}

// TextSearch sends a text request and returns the body of the Hound server response.
//
// An error is returned if there is a failure to create the request, failure to
// connect, failure to parse the response, or failure to update the conversation
// state (if applicable).
func (c *Client) TextSearch(textReq TextRequest) (string, error) {

	req, err := BuildRequest(&textReq, *c)
	if err != nil {
		return "", err
	}

	// Add the TexRequest's context to the http request
	if textReq.ctx != nil {
		req = req.WithContext(textReq.ctx)
	}

	// Set the extra client headers
	for k, v := range textReq.headers {
		req.Header.Set(k, v)
	}

	if c.HttpClient == nil {
		c.HttpClient = &http.Client{}
	}
	resp, err := c.HttpClient.Do(req)
	if err != nil {
		return "", errors.New("failed to successfully run request: " + err.Error())
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", errors.New("failed to read body: " + err.Error())
	}
	defer resp.Body.Close()

	bodyStr := string(body)

	if c.Verbose {
		fmt.Println(resp.Proto, resp.StatusCode)
		fmt.Println("Headers: ", resp.Header)
		fmt.Println(bodyStr)
	}

	//don't try to parse out conversation state from a bad response
	if resp.StatusCode >= 400 {
		return bodyStr, errors.Errorf("error response (status code: %d)", resp.StatusCode)
	}
	// update with new conversation state
	if c.enableConversationState {
		newConvState, err := parseConversationState(bodyStr)
		if err != nil {
			return bodyStr, errors.Wrap(err, "unable to parse new conversation state from response")
		}
		c.conversationState = newConvState
	}

	return bodyStr, nil
}

// VoiceSearch sends an audio request and returns the body of the Hound server response.
//
// The partialTranscriptChan parameter allows the caller to receive for PartialTranscripts
// while the Hound server is listening to the voice search. If partial transcripts are not
// needed, create a throwaway channel that listens and discards all the PartialTranscripts
// sent.
//
// An error is returned if there is a failure to create the request, failure to
// connect, failure to parse the response, or failure to update the conversation
// state (if applicable).
func (c *Client) VoiceSearch(voiceReq VoiceRequest, partialTranscriptChan chan PartialTranscript) (string, error) {
	vad := voiceReq.serverDeterminesEndOfAudio()
	fmt.Printf("[houndify-sdk] VoiceSearch starting requestId=%s holdEOF=%v (VAD=%v)\n", voiceReq.RequestID, vad, vad)
	partialsTxChan := make(chan PartialTranscript, 10)
	defer close(partialsTxChan)

	// send partials to partialTranscriptChan and close when finished
	go func() {
		defer close(partialTranscriptChan)
		for partial := range partialsTxChan {
			partialTranscriptChan <- partial
		}
	}()

	// Ensure that RequestInfoInBody isn't set for VoiceRequests because the Audio stream
	// has to go into the body
	c.RequestInfoInBody = false
	req, err := BuildRequest(&voiceReq, *c)
	if err != nil {
		return "", err
	}

	if voiceReq.ctx != nil {
		req = req.WithContext(voiceReq.ctx)
	}

	// Set the extra client headers
	for k, v := range voiceReq.headers {
		req.Header.Set(k, v)
	}

	bodyReader := newDeferredEOFReader(voiceReq.AudioStream, vad)
	req.Body = bodyReader
	defer bodyReader.ReleaseEOF()

	if c.HttpClient == nil {
		c.HttpClient = &http.Client{}
	}

	// send the request
	resp, err := c.HttpClient.Do(req)
	if err != nil {
		return "", errors.New("failed to successfully run request: " + err.Error())
	}
	defer resp.Body.Close()

	if c.Verbose {
		fmt.Println(resp.Proto, resp.StatusCode)
		fmt.Println("Headers: ", resp.Header)
	}

	var jReader jitterReader

	// Debug: optionally jitter response reads to widen race window
	if jitterStr := os.Getenv("DEBUG_RESPONSE_JITTER_MS"); jitterStr != "" {
		if ms, err := strconv.Atoi(jitterStr); err == nil && ms > 0 {
			jReader = jitterReader{
				reader:     resp.Body,
				maxJitter:  time.Duration(ms) * time.Millisecond,
				bodyReader: bodyReader,
				voiceReq:   &voiceReq,
				sts:        &atomic.Bool{},
			}
			resp.Body = &jReader
		}
	}

	// partial transcript parsing
	reader := bufio.NewReader(resp.Body)
	var line string
	for {
		bytes, err := reader.ReadBytes('\n')
		line = strings.TrimSpace(string(bytes))
		if c.Verbose {
			fmt.Println(line)
		}
		if err != nil {
			if err != io.EOF {
				fmt.Printf("[houndify-sdk] response read error requestId=%s err=%v\n", voiceReq.RequestID, err)
				return "", errors.New("error reading Houndify server response")
			}
			//EOF means this line must be the final response, done with partial transcripts
			fmt.Printf("[houndify-sdk] response EOF requestId=%s\n", voiceReq.RequestID)
			break
		}
		if line == "" {
			continue
		}
		if _, convertErr := strconv.Atoi(line); convertErr == nil {
			// this is an integer, so one of the ObjectByteCountPrefixes, skip it
			continue
		}
		// attempt to parse incoming json into partial transcript
		incoming := houndServerPartialTranscript{}
		if err := json.Unmarshal([]byte(line), &incoming); err != nil {
			fmt.Println("fail reading hound server message")
			continue
		}

		// --- DEBUG ---
		// Call hook to set reader delay and/or abort
		jReader.updateState(&incoming) // essentially a hook/copy to allow normal Abort() call but retain sleep
		// -------------

		if incoming.Format == "HoundVoiceQueryPartialTranscript" || incoming.Format == "SoundHoundVoiceSearchParialTranscript" {
			if incoming.SafeToStopAudio != nil && *incoming.SafeToStopAudio {
				fmt.Printf("[houndify-sdk] SafeToStopAudio received requestId=%s\n", voiceReq.RequestID)
				bodyReader.MarkSafeToStop()
			}

			// convert from houndify server's struct to SDK's simplified struct
			partialDuration, err := time.ParseDuration(fmt.Sprintf("%d", incoming.DurationMS) + "ms")
			if err != nil {
				fmt.Println("failed reading the time in partial transcript")
				continue
			}

			fmt.Printf("[houndify-sdk] push to partials - start requestId=%s\n", voiceReq.RequestID)
			partialsTxChan <- PartialTranscript{
				Message:         incoming.PartialTranscript,
				Duration:        partialDuration,
				Done:            incoming.Done,
				SafeToStopAudio: incoming.SafeToStopAudio,
			}
			fmt.Printf("[houndify-sdk] push to partials - end requestId=%s\n", voiceReq.RequestID)

			continue
		}
		if incoming.Format == "SoundHoundVoiceSearchResult" {
			//this line is the final response, done with partial transcripts
			fmt.Printf("[houndify-sdk] final response received requestId=%s\n", voiceReq.RequestID)
			break
		}
	}

	// Response fully consumed — release the held EOF so the writeLoop
	// can send the chunk terminator. Any resulting RST is harmless now.
	fmt.Printf("[houndify-sdk] response fully consumed, releasing EOF requestId=%s\n", voiceReq.RequestID)
	bodyReader.ReleaseEOF()

	bodyStr := line

	//don't try to parse out conversation state from a bad response
	if resp.StatusCode >= 400 {
		switch resp.StatusCode {
		case http.StatusUnauthorized:
			fallthrough
		case http.StatusForbidden:
			return bodyStr, errors.Errorf("unauthorized (status code: %d)", resp.StatusCode)
		default:
			return bodyStr, errors.Errorf("error response (status code: %d)", resp.StatusCode)
		}
	}
	// update with new conversation state
	if c.enableConversationState {
		newConvState, err := parseConversationState(bodyStr)
		if err != nil {
			return bodyStr, errors.Wrap(err, "unable to parse new conversation state from response")
		}
		c.conversationState = newConvState
	}

	return bodyStr, nil
}

// deferredEOFReader wraps an io.Reader and controls when the HTTP transport
// sees EOF on the request body.
//
// When holdEOF is true (VAD / ServerDeterminesEndOfAudio) AND the server has
// sent SafeToStopAudio, a real EOF from the underlying reader is held until
// ReleaseEOF() is called. This lets the response be fully read before the
// chunk terminator is sent, preventing the server-side RST from destroying
// in-flight response data.
//
// If EOF arrives before SafeToStopAudio, it passes through immediately so the
// server can process the complete audio and produce a response.
type deferredEOFReader struct {
	reader             io.Reader
	holdEOF            bool
	safeToStopReceived atomic.Bool
	responseComplete   chan struct{}
	released           atomic.Bool
}

func newDeferredEOFReader(r io.Reader, holdEOF bool) *deferredEOFReader {
	return &deferredEOFReader{
		reader:           r,
		holdEOF:          holdEOF,
		responseComplete: make(chan struct{}),
	}
}

func (a *deferredEOFReader) Read(p []byte) (int, error) {
	n, err := a.reader.Read(p)
	if err == io.EOF {
		if a.holdEOF && a.safeToStopReceived.Load() {
			// Audio is exhausted and the server already said it has enough audio.
			// Keep the chunk stream open until the response is fully read.
			fmt.Println("[houndify-sdk] audio EOF received, holding until response is fully read")
			<-a.responseComplete
			fmt.Println("[houndify-sdk] audio EOF released, chunk terminator will be sent")
		} else if a.holdEOF {
			fmt.Println("[houndify-sdk] audio EOF received before SafeToStopAudio, passing through")
		} else {
			fmt.Println("[houndify-sdk] audio EOF received (no VAD), passing through")
		}
	}
	return n, err
}

func (a *deferredEOFReader) Close() error {
	return nil
}

// MarkSafeToStop records that the server sent SafeToStopAudio. If holdEOF is
// enabled and a real EOF arrives after this point, Read() will block until
// ReleaseEOF() is called.
func (a *deferredEOFReader) MarkSafeToStop() {
	a.safeToStopReceived.Store(true)
}

// ReleaseEOF unblocks any Read() call that is holding a real EOF.
// Safe to call multiple times.
func (a *deferredEOFReader) ReleaseEOF() {
	if a.released.CompareAndSwap(false, true) {
		fmt.Printf("[houndify-sdk] ReleaseEOF called holdEOF=%v safeToStop=%v\n", a.holdEOF, a.safeToStopReceived.Load())
		close(a.responseComplete)
	}
}

// jitterReader wraps an io.ReadCloser and adds a random sleep before each Read,
// used only for debugging to widen race windows.
type jitterReader struct {
	reader     io.ReadCloser
	maxJitter  time.Duration
	bodyReader *deferredEOFReader
	voiceReq   *VoiceRequest
	sts        *atomic.Bool
}

func (j *jitterReader) Read(p []byte) (int, error) {
	n, err := j.reader.Read(p)

	// --- DEBUG ---
	line := strings.TrimSpace(string(p))
	fmt.Println(line)
	// -------------

	if j.sts.Load() {
		jitter := rand.Int63n(int64(j.maxJitter))
		fmt.Printf("-- DEBUG -- Adding artificial delay before processing STS partial response..  jitter=%d, maxJitter=%s\n", jitter, j.maxJitter)
		time.Sleep(time.Duration(jitter))
		fmt.Printf("-- DEBUG -- Done with artificial delay..  jitter=%d, maxJitter=%s\n", jitter, j.maxJitter)
	}

	return n, err
}

// -- TEST INSTRUMENTATION --
// Ensure that we properly test same error-inducing scenario but with our Abort logic.
// To keep the real code above untouched, this func duplicates the same logic to determine
// when to call Abort(), but the read delay is maintained as to allow the same race window
// in the real response read loop
func (j *jitterReader) updateState(incoming *houndServerPartialTranscript) {
	fmt.Println("-- DEBUG -- updateState eval")
	if incoming.Format == "HoundVoiceQueryPartialTranscript" || incoming.Format == "SoundHoundVoiceSearchParialTranscript" {
		if incoming.SafeToStopAudio != nil && *incoming.SafeToStopAudio && j.voiceReq.serverDeterminesEndOfAudio() {
			fmt.Println("-- DEBUG -- SafeToStopAudio seen, enabling jitter delay")
			j.sts.Store(true)
		}
	}
}

func (j *jitterReader) Close() error {
	return j.reader.Close()
}
