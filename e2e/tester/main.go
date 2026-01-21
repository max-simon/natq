package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	natsURL        = "nats://localhost:4222"
	requestTimeout = 5 * time.Second
	kvPollInterval = 100 * time.Millisecond
	kvPollTimeout  = 30 * time.Second
)

// TaskRunOutput represents the output from a task execution
type TaskRunOutput struct {
	ID     string      `json:"id"`
	TaskID string      `json:"taskId"`
	Status int         `json:"status"`
	Data   interface{} `json:"data,omitempty"`
	Error  string      `json:"error,omitempty"`
}

// TestResult holds the result of a single test
type TestResult struct {
	Name    string
	Passed  bool
	Message string
}

func main() {
	fmt.Println("=== natq E2E Test Runner ===")
	fmt.Println()

	// Connect to NATS
	nc, err := nats.Connect(natsURL)
	if err != nil {
		fmt.Printf("Failed to connect to NATS: %v\n", err)
		os.Exit(1)
	}
	defer nc.Close()

	// Get JetStream context
	js, err := nc.JetStream()
	if err != nil {
		fmt.Printf("Failed to get JetStream context: %v\n", err)
		os.Exit(1)
	}

	// Get KV bucket for results
	kv, err := js.KeyValue("natq_results")
	if err != nil {
		fmt.Printf("Failed to get KV bucket (is a worker running?): %v\n", err)
		os.Exit(1)
	}

	var results []TestResult

	// Run sync tests
	results = append(results, testSyncAdd(nc))
	results = append(results, testSyncEcho(nc))
	results = append(results, testSyncClientError(nc))

	// Run async tests
	results = append(results, testAsyncDelay(js, kv))
	results = append(results, testAsyncRetry(js, kv))
	results = append(results, testAsyncClientError(js, kv))
	results = append(results, testAsyncDropResult(js, kv))

	// Print results
	fmt.Println()
	fmt.Println("=== Test Results ===")
	fmt.Println()

	passed := 0
	failed := 0
	for _, r := range results {
		status := "PASS"
		if !r.Passed {
			status = "FAIL"
			failed++
		} else {
			passed++
		}
		fmt.Printf("[%s] %s\n", status, r.Name)
		if r.Message != "" {
			fmt.Printf("       %s\n", r.Message)
		}
	}

	fmt.Println()
	fmt.Printf("Total: %d passed, %d failed\n", passed, failed)

	if failed > 0 {
		os.Exit(1)
	}
}

// testSyncAdd tests the e2e-add sync task
func testSyncAdd(nc *nats.Conn) TestResult {
	name := "Sync: Add Numbers (e2e-add)"

	input := map[string]interface{}{
		"a": 5,
		"b": 3,
	}
	inputBytes, _ := json.Marshal(input)

	msg, err := nc.Request("natq.req.e2e-add", inputBytes, requestTimeout)
	if err != nil {
		return TestResult{Name: name, Passed: false, Message: fmt.Sprintf("Request failed: %v", err)}
	}

	// Check status header
	status := msg.Header.Get("status")
	if status != "200" {
		return TestResult{Name: name, Passed: false, Message: fmt.Sprintf("Expected status 200, got %s", status)}
	}

	// Parse response data
	var data map[string]interface{}
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		return TestResult{Name: name, Passed: false, Message: fmt.Sprintf("Failed to parse response: %v", err)}
	}

	sum, ok := data["sum"].(float64)
	if !ok || sum != 8 {
		return TestResult{Name: name, Passed: false, Message: fmt.Sprintf("Expected sum=8, got %v", data["sum"])}
	}

	return TestResult{Name: name, Passed: true}
}

// testSyncEcho tests the e2e-echo sync task
func testSyncEcho(nc *nats.Conn) TestResult {
	name := "Sync: Echo (e2e-echo)"

	input := map[string]interface{}{
		"message": "hello world",
		"nested":  map[string]interface{}{"foo": "bar"},
	}
	inputBytes, _ := json.Marshal(input)

	msg, err := nc.Request("natq.req.e2e-echo", inputBytes, requestTimeout)
	if err != nil {
		return TestResult{Name: name, Passed: false, Message: fmt.Sprintf("Request failed: %v", err)}
	}

	status := msg.Header.Get("status")
	if status != "200" {
		return TestResult{Name: name, Passed: false, Message: fmt.Sprintf("Expected status 200, got %s", status)}
	}

	var data map[string]interface{}
	if err := json.Unmarshal(msg.Data, &data); err != nil {
		return TestResult{Name: name, Passed: false, Message: fmt.Sprintf("Failed to parse response: %v", err)}
	}

	message, _ := data["message"].(string)
	if message != "hello world" {
		return TestResult{Name: name, Passed: false, Message: fmt.Sprintf("Expected message='hello world', got %v", data["message"])}
	}

	nested, ok := data["nested"].(map[string]interface{})
	if !ok {
		return TestResult{Name: name, Passed: false, Message: "Expected nested object"}
	}
	if nested["foo"] != "bar" {
		return TestResult{Name: name, Passed: false, Message: fmt.Sprintf("Expected nested.foo='bar', got %v", nested["foo"])}
	}

	return TestResult{Name: name, Passed: true}
}

// testSyncClientError tests the e2e-client-error sync task
func testSyncClientError(nc *nats.Conn) TestResult {
	name := "Sync: Client Error (e2e-client-error)"

	input := map[string]interface{}{
		"shouldFail": true,
	}
	inputBytes, _ := json.Marshal(input)

	msg, err := nc.Request("natq.req.e2e-client-error", inputBytes, requestTimeout)
	if err != nil {
		return TestResult{Name: name, Passed: false, Message: fmt.Sprintf("Request failed: %v", err)}
	}

	status := msg.Header.Get("status")
	if status != "400" {
		return TestResult{Name: name, Passed: false, Message: fmt.Sprintf("Expected status 400, got %s", status)}
	}

	errorMsg := msg.Header.Get("error")
	if errorMsg != "Client requested failure" {
		return TestResult{Name: name, Passed: false, Message: fmt.Sprintf("Expected error 'Client requested failure', got '%s'", errorMsg)}
	}

	return TestResult{Name: name, Passed: true}
}

// testAsyncDelay tests the e2e-delay async task
func testAsyncDelay(js nats.JetStreamContext, kv nats.KeyValue) TestResult {
	name := "Async: Delayed Response (e2e-delay)"

	runID := fmt.Sprintf("delay-test-%d", time.Now().UnixNano())
	kvKey := fmt.Sprintf("e2e-delay.%s", runID)

	input := map[string]interface{}{
		"runId":   runID,
		"delayMs": 500,
	}
	inputBytes, _ := json.Marshal(input)

	// Publish to JetStream
	_, err := js.Publish("natq.job.e2e-delay", inputBytes)
	if err != nil {
		return TestResult{Name: name, Passed: false, Message: fmt.Sprintf("Failed to publish: %v", err)}
	}

	// Poll for initial PROCESSING status
	var sawProcessing bool
	var output TaskRunOutput

	deadline := time.Now().Add(kvPollTimeout)
	for time.Now().Before(deadline) {
		entry, err := kv.Get(kvKey)
		if err != nil {
			time.Sleep(kvPollInterval)
			continue
		}

		if err := json.Unmarshal(entry.Value(), &output); err != nil {
			return TestResult{Name: name, Passed: false, Message: fmt.Sprintf("Failed to parse KV entry: %v", err)}
		}

		if output.Status == 100 {
			sawProcessing = true
		}

		if output.Status == 200 {
			break
		}

		time.Sleep(kvPollInterval)
	}

	if !sawProcessing {
		return TestResult{Name: name, Passed: false, Message: "Never saw status 100 (PROCESSING)"}
	}

	if output.Status != 200 {
		return TestResult{Name: name, Passed: false, Message: fmt.Sprintf("Expected final status 200, got %d", output.Status)}
	}

	data, ok := output.Data.(map[string]interface{})
	if !ok || data["delayed"] != true {
		return TestResult{Name: name, Passed: false, Message: fmt.Sprintf("Expected data.delayed=true, got %v", output.Data)}
	}

	return TestResult{Name: name, Passed: true}
}

// testAsyncRetry tests the e2e-retry async task
func testAsyncRetry(js nats.JetStreamContext, kv nats.KeyValue) TestResult {
	name := "Async: Server Error with Retry (e2e-retry)"

	runID := fmt.Sprintf("retry-test-%d", time.Now().UnixNano())
	kvKey := fmt.Sprintf("e2e-retry.%s", runID)

	input := map[string]interface{}{
		"runId":     runID,
		"failCount": 2,
	}
	inputBytes, _ := json.Marshal(input)

	_, err := js.Publish("natq.job.e2e-retry", inputBytes)
	if err != nil {
		return TestResult{Name: name, Passed: false, Message: fmt.Sprintf("Failed to publish: %v", err)}
	}

	var output TaskRunOutput
	deadline := time.Now().Add(kvPollTimeout)
	for time.Now().Before(deadline) {
		entry, err := kv.Get(kvKey)
		if err != nil {
			time.Sleep(kvPollInterval)
			continue
		}

		if err := json.Unmarshal(entry.Value(), &output); err != nil {
			return TestResult{Name: name, Passed: false, Message: fmt.Sprintf("Failed to parse KV entry: %v", err)}
		}

		// During retries, status should remain at 100
		if output.Status == 200 {
			break
		}

		if output.Status != 100 {
			return TestResult{Name: name, Passed: false, Message: fmt.Sprintf("Expected status 100 during retry, got %d", output.Status)}
		}

		time.Sleep(kvPollInterval)
	}

	if output.Status != 200 {
		return TestResult{Name: name, Passed: false, Message: fmt.Sprintf("Expected final status 200, got %d", output.Status)}
	}

	data, ok := output.Data.(map[string]interface{})
	if !ok {
		return TestResult{Name: name, Passed: false, Message: "Expected data object"}
	}

	attempts, ok := data["attempts"].(float64)
	if !ok || attempts < 2 {
		return TestResult{Name: name, Passed: false, Message: fmt.Sprintf("Expected attempts >= 2, got %v", data["attempts"])}
	}

	return TestResult{Name: name, Passed: true}
}

// testAsyncClientError tests the e2e-async-client-error async task
func testAsyncClientError(js nats.JetStreamContext, kv nats.KeyValue) TestResult {
	name := "Async: Client Error (e2e-async-client-error)"

	runID := fmt.Sprintf("async-error-%d", time.Now().UnixNano())
	kvKey := fmt.Sprintf("e2e-async-client-error.%s", runID)

	input := map[string]interface{}{
		"runId": runID,
	}
	inputBytes, _ := json.Marshal(input)

	_, err := js.Publish("natq.job.e2e-async-client-error", inputBytes)
	if err != nil {
		return TestResult{Name: name, Passed: false, Message: fmt.Sprintf("Failed to publish: %v", err)}
	}

	var output TaskRunOutput
	deadline := time.Now().Add(kvPollTimeout)
	for time.Now().Before(deadline) {
		entry, err := kv.Get(kvKey)
		if err != nil {
			time.Sleep(kvPollInterval)
			continue
		}

		if err := json.Unmarshal(entry.Value(), &output); err != nil {
			return TestResult{Name: name, Passed: false, Message: fmt.Sprintf("Failed to parse KV entry: %v", err)}
		}

		// Should go from 100 to 400, not retry
		if output.Status == 400 {
			break
		}

		time.Sleep(kvPollInterval)
	}

	if output.Status != 400 {
		return TestResult{Name: name, Passed: false, Message: fmt.Sprintf("Expected status 400, got %d", output.Status)}
	}

	if output.Error != "Async client error" {
		return TestResult{Name: name, Passed: false, Message: fmt.Sprintf("Expected error 'Async client error', got '%s'", output.Error)}
	}

	return TestResult{Name: name, Passed: true}
}

// testAsyncDropResult tests the e2e-drop-result async task with dropResultOnSuccess
func testAsyncDropResult(js nats.JetStreamContext, kv nats.KeyValue) TestResult {
	name := "Async: Drop Result on Success (e2e-drop-result)"

	runID := fmt.Sprintf("drop-test-%d", time.Now().UnixNano())
	kvKey := fmt.Sprintf("e2e-drop-result.%s", runID)

	input := map[string]interface{}{
		"runId":               runID,
		"dropResultOnSuccess": true,
	}
	inputBytes, _ := json.Marshal(input)

	// Publish to JetStream
	_, err := js.Publish("natq.job.e2e-drop-result", inputBytes)
	if err != nil {
		return TestResult{Name: name, Passed: false, Message: fmt.Sprintf("Failed to publish: %v", err)}
	}

	// Poll for result - we should see status 200 briefly, then key should be deleted
	var sawProcessing bool
	var sawDeleted bool

	deadline := time.Now().Add(kvPollTimeout)
	for time.Now().Before(deadline) {
		entry, err := kv.Get(kvKey)
		if err != nil {
			// Key not found - could be deleted or not yet created
			if sawProcessing {
				// We saw success before, now it's gone - that's the expected behavior
				sawDeleted = true
				break
			}
			time.Sleep(kvPollInterval)
			continue
		}

		var output TaskRunOutput
		if err := json.Unmarshal(entry.Value(), &output); err != nil {
			return TestResult{Name: name, Passed: false, Message: fmt.Sprintf("Failed to parse KV entry: %v", err)}
		}

		if output.Status == 100 {
			sawProcessing = true
			// Continue polling to see if it gets deleted
		}

		time.Sleep(kvPollInterval)
	}

	if !sawProcessing {
		return TestResult{Name: name, Passed: false, Message: "Never saw status 100 (processing)"}
	}

	if !sawDeleted {
		return TestResult{Name: name, Passed: false, Message: "Result was not deleted from KV after success"}
	}

	return TestResult{Name: name, Passed: true}
}
