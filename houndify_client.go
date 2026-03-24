package houndify

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
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

	// Add the TexRequest's context to the http request
	if textReq.ctx != nil {
		req = req.WithContext(textReq.ctx)
	}

	// Set the extra client headers
	for k, v := range textReq.headers {
		req.Header.Set(k, v)
	}

	if err != nil {
		return "", err
	}

	if c.HttpClient == nil {
		c.HttpClient = &http.Client{}
	}
	resp, err := c.HttpClient.Do(req)
	if err != nil {
		return "", errors.New("failed to successfully run request: " + err.Error())
	}

	body, err := ioutil.ReadAll(resp.Body)
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
		return bodyStr, errors.New("error response")
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

	//so the partial transcript channel doesn't get closed before all transcripts are sent
	partialChanWait := sync.WaitGroup{}

	defer func() {
		go func() {
			//don't close the open partial transcript channel
			partialChanWait.Wait()
			close(partialTranscriptChan)
		}()
	}()

	// Ensure that RequestInfoInBody isn't set for VoiceRequests because the Audio stream
	// has to go into the body
	c.RequestInfoInBody = false
	req, err := BuildRequest(&voiceReq, *c)
	if voiceReq.ctx != nil {
		req = req.WithContext(voiceReq.ctx)
	}

	// Set the extra client headers
	for k, v := range voiceReq.headers {
		req.Header.Set(k, v)
	}

	if err != nil {
		return "", err
	}
	req.Body = ioutil.NopCloser(&debugAudioReader{
		reader:    voiceReq.AudioStream,
		requestID: voiceReq.RequestID,
	})

	if c.HttpClient == nil {
		c.HttpClient = &http.Client{}
	}

	// send the request
	resp, err := c.HttpClient.Do(req)
	if err != nil {
		return "", errors.New("failed to successfully run request: " + err.Error())
	}

	fmt.Printf("[houndify-sdk] RESPONSE STREAM OPEN requestId=%s status=%d time=%s\n",
		voiceReq.RequestID, resp.StatusCode, time.Now().Format(time.StampMicro))

	if c.Verbose {
		fmt.Println(resp.Proto, resp.StatusCode)
		fmt.Println("Headers: ", resp.Header)
	}

	// Debug: optionally jitter response reads to widen race window
	if jitterStr := os.Getenv("DEBUG_RESPONSE_JITTER_MS"); jitterStr != "" {
		if ms, err := strconv.Atoi(jitterStr); err == nil && ms > 0 {
			resp.Body = &jitterReader{
				reader: resp.Body,
				jitter: time.Duration(ms) * time.Millisecond,
			}
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
				fmt.Printf("[houndify-sdk] RESPONSE READ ERROR requestId=%s err=%v errType=%T time=%s\n",
					voiceReq.RequestID, err, err, time.Now().Format(time.StampMicro))
				return "", errors.New("error reading Houndify server response")
			}
			//EOF means this line must be the final response, done with partial transcripts
			fmt.Printf("[houndify-sdk] RESPONSE EOF requestId=%s time=%s\n",
				voiceReq.RequestID, time.Now().Format(time.StampMicro))
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
		if incoming.Format == "HoundVoiceQueryPartialTranscript" || incoming.Format == "SoundHoundVoiceSearchParialTranscript" {
			fmt.Printf("[houndify-sdk] PARTIAL requestId=%s transcript=%q safeToStop=%v time=%s\n",
				voiceReq.RequestID, incoming.PartialTranscript, *incoming.SafeToStopAudio, time.Now().Format(time.StampMicro))
			// convert from houndify server's struct to SDK's simplified struct
			partialDuration, err := time.ParseDuration(fmt.Sprintf("%d", incoming.DurationMS) + "ms")
			if err != nil {
				fmt.Println("failed reading the time in partial transcript")
				continue
			}
			partialChanWait.Add(1)
			go func() {
				partialTranscriptChan <- PartialTranscript{
					Message:         incoming.PartialTranscript,
					Duration:        partialDuration,
					Done:            incoming.Done,
					SafeToStopAudio: incoming.SafeToStopAudio,
				}
				partialChanWait.Done()
			}()
			continue
		}
		if incoming.Format == "SoundHoundVoiceSearchResult" {
			fmt.Printf("[houndify-sdk] FINAL RESPONSE requestId=%s lineLen=%d time=%s\n",
				voiceReq.RequestID, len(line), time.Now().Format(time.StampMicro))
			//this line is the final response, done with partial transcripts
			break
		}
	}

	bodyStr := line
	defer resp.Body.Close()

	//don't try to parse out conversation state from a bad response
	if resp.StatusCode >= 400 {
		switch resp.StatusCode {
		case http.StatusUnauthorized:
		case http.StatusForbidden:
			return bodyStr, errors.New("unauthorized")
		default:
			return bodyStr, errors.New("error response")
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

	fmt.Printf("[houndify-sdk] VOICESEARCH COMPLETE requestId=%s bodyLen=%d time=%s\n",
		voiceReq.RequestID, len(bodyStr), time.Now().Format(time.StampMicro))

	return bodyStr, nil
}

// debugAudioReader wraps an io.Reader to log audio read activity for the HTTP
// transport's writeLoop. This lets us see whether audio is still being sent
// after SafeToStopAudio and correlate timestamps with response reads.
type debugAudioReader struct {
	reader    io.Reader
	requestID string
	total     int64
	reads     int
}

func (d *debugAudioReader) Read(p []byte) (int, error) {
	n, err := d.reader.Read(p)
	d.total += int64(n)
	d.reads++
	if err == io.EOF {
		fmt.Printf("[houndify-sdk] AUDIO READ EOF requestId=%s reads=%d totalBytes=%d time=%s\n",
			d.requestID, d.reads, d.total, time.Now().Format(time.StampMicro))
	} else if err != nil {
		fmt.Printf("[houndify-sdk] AUDIO READ ERROR requestId=%s reads=%d totalBytes=%d err=%v time=%s\n",
			d.requestID, d.reads, d.total, err, time.Now().Format(time.StampMicro))
	} else if d.reads%50 == 0 {
		fmt.Printf("[houndify-sdk] AUDIO READ requestId=%s reads=%d totalBytes=%d n=%d time=%s\n",
			d.requestID, d.reads, d.total, n, time.Now().Format(time.StampMicro))
	}
	return n, err
}

// jitterReader wraps an io.ReadCloser and adds a random sleep before each Read,
// used only for debugging to widen race windows.
type jitterReader struct {
	reader io.ReadCloser
	jitter time.Duration
}

func (j *jitterReader) Read(p []byte) (int, error) {
	t0 := time.Now()
	fmt.Printf("[houndify-sdk] JITTER READ start --> delay=%s readTime=%s time=%s\n", j.jitter, time.Since(t0), time.Now().Format(time.StampMicro))
	time.Sleep(j.jitter)
	n, err := j.reader.Read(p)
	fmt.Printf("[houndify-sdk] JITTER READ end <-- n=%d err=%v delay=%s readTime=%s time=%s\n", n, err, j.jitter, time.Since(t0), time.Now().Format(time.StampMicro))
	return n, err
}

func (j *jitterReader) Close() error {
	return j.reader.Close()
}
