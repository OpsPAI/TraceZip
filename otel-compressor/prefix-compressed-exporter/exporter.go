package prefix_compressed_exporter // import "go.opentelemetry.io/collector/exporter/otlpexporter"

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/dsnet/compress/bzip2"
	"github.com/ulikunitz/xz/lzma"
	"go.uber.org/zap"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
)

type baseExporter struct {
	// Input configuration.
	config        *Config
	client        *http.Client
	tracesURL     string
	tracesdictURL string
	metricsURL    string
	logsURL       string
	logger        *zap.Logger
	settings      component.TelemetrySettings
	// Default user-agent header.
	userAgent string
}

type CompressorStat struct {
	gzipOnlyTotal      int
	mergingNoGzipTotal int
	mergingTotal       int
	originTotal        int
	lzmaTotal          int
	bzipTotal          int
	mergingLzmaTotal   int
	mergingBzipTotal   int
}

func CompressLZMA(data []byte) (int, error) {
	var compressedData bytes.Buffer
	writer, err := lzma.NewWriter(&compressedData)
	if err != nil {
		return 0, err
	}
	_, err = writer.Write(data)
	if err != nil {
		return 0, err
	}
	err = writer.Close()
	if err != nil {
		return 0, err
	}
	return len(compressedData.Bytes()), nil
}

// CompressBZIP2 compresses the input data using BZIP2 and returns the compressed data.
func CompressBZIP2(data []byte) (int, error) {
	var compressedData bytes.Buffer
	writer, err := bzip2.NewWriter(&compressedData, nil)
	if err != nil {
		return 0, err
	}
	_, err = writer.Write(data)
	if err != nil {
		return 0, err
	}
	err = writer.Close()
	if err != nil {
		return 0, err
	}
	return len(compressedData.Bytes()), nil
}

var compressorStat CompressorStat

var needResetOrder = false

var CompressionTotalTime time.Duration

var GzipTotalTime time.Duration

var GzipOnlyTotalTime time.Duration

var GzipOnlyMarshalTime time.Duration

var DictSizeNow = 0

const (
	headerRetryAfter         = "Retry-After"
	maxHTTPResponseReadBytes = 64 * 1024

	jsonContentType     = "application/json"
	protobufContentType = "application/x-protobuf"
)

// Create new exporter.
func newExporter(cfg component.Config, set exporter.CreateSettings) (*baseExporter, error) {
	oCfg := cfg.(*Config)

	if oCfg.Endpoint != "" {
		_, err := url.Parse(oCfg.Endpoint)
		if err != nil {
			return nil, errors.New("endpoint must be a valid URL")
		}
	}

	userAgent := fmt.Sprintf("%s/%s (%s/%s)",
		set.BuildInfo.Description, set.BuildInfo.Version, runtime.GOOS, runtime.GOARCH)

	// client construction is deferred to start
	return &baseExporter{
		config:    oCfg,
		logger:    set.Logger,
		userAgent: userAgent,
		settings:  set.TelemetrySettings,
	}, nil
}

// start actually creates the HTTP client. The client construction is deferred till this point as this
// is the only place we get hold of Extensions which are required to construct auth round tripper.
func (e *baseExporter) start(_ context.Context, host component.Host) error {
	client, err := e.config.ClientConfig.ToClient(host, e.settings)
	if err != nil {
		return err
	}
	e.client = client
	return nil
}

var DictRWM sync.RWMutex

var SerilizeLock sync.Mutex

func (e *baseExporter) pushTraces(ctx context.Context, td ptrace.Traces) error {

	if e.config.CalcZipRate {
		SerilizeLock.Lock()
		defer SerilizeLock.Unlock()
	}

	tr := ptraceotlp.NewExportRequestFromTraces(td)

	if e.config.NoTraceZip && e.config.EnableGzip {
		var buf bytes.Buffer
		orig, _ := tr.MarshalJSON()
		gz := gzip.NewWriter(&buf)
		if _, err := gz.Write(orig); err != nil {
			return err
		}
		if err := gz.Close(); err != nil {
			return err
		}
		return e.export(ctx, e.tracesURL, buf.Bytes(), e.logsPartialSuccessHandler)
	}

	if e.config.NoTraceZip {
		orig, _ := tr.MarshalJSON()
		return e.export(ctx, e.tracesURL, orig, e.logsPartialSuccessHandler)
	}

	var err error
	var request []byte
	isUpdate := false
	UpdateWrapper := make(map[string]interface{})
	ExportWrapper := make(map[string]interface{})
	var export interface{}
	var subeteUpdate, incrementUpdate []interface{}
	var dictionaryUuid string
	switch e.config.Encoding {
	case EncodingJSON:
		start := time.Now()
		DictRWM.Lock()
		dictionaryUuid, subeteUpdate, incrementUpdate, export = tr.MarshalWithTraceZip(e.config.TrieBuffer, e.config.AttrLimit, e.config.ThresholdRate, needResetOrder, e.config.DeleteResource)
		CompressionTotalTime += time.Since(start)
		needResetOrder = false
	case EncodingProto:
		request, err = tr.MarshalProto()
	default:
		err = fmt.Errorf("invalid encoding: %s", e.config.Encoding)
	}
	if e.config.Encoding == EncodingJSON {
		UpdateWrapper["_"] = dictionaryUuid
		ExportWrapper["_"] = dictionaryUuid
		ExportWrapper["a"] = export
		request, _ = json.Marshal(ExportWrapper)
		if len(subeteUpdate) > 0 {
			UpdateWrapper["t"] = "a"
			UpdateWrapper["n"] = subeteUpdate
			isUpdate = true
		} else if len(incrementUpdate) > 0 {
			UpdateWrapper["t"] = "i"
			UpdateWrapper["n"] = incrementUpdate
			isUpdate = true
		}

		if err != nil {
			return consumererror.NewPermanent(err)
		}

		if isUpdate {
			// Update
			dictJson, err := json.Marshal(UpdateWrapper)
			if err != nil {
				return err
			}

			if len(subeteUpdate) > 0 {
				DictSizeNow = len(dictJson)
			} else {
				DictSizeNow += len(dictJson)
			}

			var reqBody *bytes.Buffer
			var contentType string

			if e.config.EnableGzip {
				var buf bytes.Buffer
				gz := gzip.NewWriter(&buf)
				start := time.Now()
				if _, err := gz.Write(dictJson); err != nil {
					return err
				}
				if err := gz.Close(); err != nil {
					return err
				}
				GzipTotalTime += time.Since(start)
				reqBody = &buf
				contentType = "application/json"
			} else {
				reqBody = bytes.NewBuffer(dictJson)
				contentType = "application/json"
			}

			if e.config.CalcZipRate {
				var buf bytes.Buffer
				start := time.Now()
				orig, _ := tr.MarshalJSON__(e.config.DeleteResource)
				GzipOnlyMarshalTime += time.Since(start)
				gz := gzip.NewWriter(&buf)
				if _, err := gz.Write(orig); err != nil {
					return err
				}
				if err := gz.Close(); err != nil {
					return err
				}
				GzipOnlyTotalTime += time.Since(start)
				compressorStat.mergingTotal += reqBody.Len()
				compressorStat.mergingNoGzipTotal += len(dictJson)
				compressorStat.originTotal += len(orig)
				compressorStat.gzipOnlyTotal += buf.Len()
				bzip, _ := CompressBZIP2(orig)
				compressorStat.bzipTotal += bzip
				lzma, _ := CompressLZMA(orig)
				compressorStat.lzmaTotal += lzma
				bzip, _ = CompressBZIP2(dictJson)
				compressorStat.mergingBzipTotal += bzip
				lzma, _ = CompressLZMA(dictJson)
				compressorStat.mergingLzmaTotal += lzma
			}

			if e.config.EnableGzip && e.config.CalcZipRate {
				ratio := float64(compressorStat.mergingTotal) / float64(compressorStat.originTotal)
				traceZipCost := float64(CompressionTotalTime.Seconds()) - (float64(GzipOnlyMarshalTime.Seconds()) * ratio)
				fmt.Printf("[TraceZip + Gzip Speed] %f MB/s\n", float64(compressorStat.originTotal)/float64(GzipTotalTime.Seconds()+CompressionTotalTime.Seconds())/1024/1024)
				fmt.Printf("[TraceZip        Speed] %f MB/s\n", float64(compressorStat.originTotal)/float64(CompressionTotalTime.Seconds())/1024/1024)
				fmt.Printf("[Gzip            Speed] %f MB/s\n", float64(compressorStat.originTotal)/float64(GzipOnlyTotalTime.Seconds())/1024/1024)
				fmt.Printf("[NoZip           Speed] %f MB/s\n", float64(compressorStat.originTotal)/float64(GzipOnlyMarshalTime.Seconds())/1024/1024)
				fmt.Printf("[TraceZip         Cost] %f MB/s\n", float64(compressorStat.originTotal)/float64(traceZipCost)/1024/1024)
			}

			req, err := http.NewRequest("POST", e.tracesdictURL, reqBody)
			if err != nil {
				needResetOrder = true
				DictRWM.Unlock()
				return errors.New("failed to create request")
			}
			req.Header.Set("Content-Type", contentType)
			if e.config.EnableGzip {
				req.Header.Set("Content-Encoding", "gzip")
			}

			client := &http.Client{}
			rsp, err := client.Do(req)
			if err != nil {
				needResetOrder = true
				DictRWM.Unlock()
				return errors.New("synchronize dictionary failed")
			}
			defer rsp.Body.Close()
			if rsp.StatusCode < 400 && rsp.StatusCode >= 200 {
				DictRWM.Unlock()
				content, err := io.ReadAll(rsp.Body)
				if err != nil {
					return err
				}
				fmt.Println(string(content))
				return e.export(ctx, e.tracesURL, request, e.tracesPartialSuccessHandler)
			} else {
				needResetOrder = true
				DictRWM.Unlock()
				return errors.New("synchronize dictionary failed")
			}
		} else {
			DictRWM.Unlock()
			// No Update
			if e.config.CalcZipRate {
				var buf bytes.Buffer
				start := time.Now()
				orig, _ := tr.MarshalJSON__(e.config.DeleteResource)
				GzipOnlyMarshalTime += time.Since(start)
				gz, _ := gzip.NewWriterLevel(&buf, 6)
				if _, err := gz.Write(orig); err != nil {
					return err
				}
				if err := gz.Close(); err != nil {
					return err
				}
				GzipOnlyTotalTime += time.Since(start)
				compressorStat.originTotal += len(orig)
				compressorStat.gzipOnlyTotal += buf.Len()
				bzip, _ := CompressBZIP2(orig)
				compressorStat.bzipTotal += bzip
				lzma, _ := CompressLZMA(orig)
				compressorStat.lzmaTotal += lzma

				if e.config.EnableGzip && e.config.CalcZipRate {
					ratio := float64(compressorStat.mergingTotal) / float64(compressorStat.originTotal)
					traceZipCost := float64(CompressionTotalTime.Seconds()) - (float64(GzipOnlyMarshalTime.Seconds()) * ratio)
					fmt.Printf("[TraceZip + Gzip Speed] %f MB/s\n", float64(compressorStat.originTotal)/float64(GzipTotalTime.Seconds()+CompressionTotalTime.Seconds())/1024/1024)
					fmt.Printf("[TraceZip        Speed] %f MB/s\n", float64(compressorStat.originTotal)/float64(CompressionTotalTime.Seconds())/1024/1024)
					fmt.Printf("[Gzip            Speed] %f MB/s\n", float64(compressorStat.originTotal)/float64(GzipOnlyTotalTime.Seconds())/1024/1024)
					fmt.Printf("[NoZip           Speed] %f MB/s\n", float64(compressorStat.originTotal)/float64(GzipOnlyMarshalTime.Seconds())/1024/1024)
					fmt.Printf("[TraceZip         Cost] %f MB/s\n", float64(compressorStat.originTotal)/float64(traceZipCost)/1024/1024)
				}
			}
		}
		return e.export(ctx, e.tracesURL, request, e.tracesPartialSuccessHandler)
	} else {
		return e.export(ctx, e.tracesURL, request, e.tracesPartialSuccessHandler)
	}

}

func (e *baseExporter) pushMetrics(ctx context.Context, md pmetric.Metrics) error {
	tr := pmetricotlp.NewExportRequestFromMetrics(md)

	var err error
	var request []byte
	switch e.config.Encoding {
	case EncodingJSON:
		request, err = tr.MarshalJSON()
	case EncodingProto:
		request, err = tr.MarshalProto()
	default:
		err = fmt.Errorf("invalid encoding: %s", e.config.Encoding)
	}

	if err != nil {
		return consumererror.NewPermanent(err)
	}
	return e.export(ctx, e.metricsURL, request, e.metricsPartialSuccessHandler)
}

func (e *baseExporter) pushLogs(ctx context.Context, ld plog.Logs) error {
	tr := plogotlp.NewExportRequestFromLogs(ld)

	var err error
	var request []byte
	switch e.config.Encoding {
	case EncodingJSON:
		request, err = tr.MarshalJSON()
	case EncodingProto:
		request, err = tr.MarshalProto()
	default:
		err = fmt.Errorf("invalid encoding: %s", e.config.Encoding)
	}

	if err != nil {
		return consumererror.NewPermanent(err)
	}

	return e.export(ctx, e.logsURL, request, e.logsPartialSuccessHandler)
}

func (e *baseExporter) export(ctx context.Context, url string, request []byte, partialSuccessHandler partialSuccessHandler) error {
	e.logger.Debug("Preparing to make HTTP request", zap.String("url", url))

	var bodyReader *bytes.Reader

	if e.config.EnableGzip && !e.config.NoTraceZip {
		var buf bytes.Buffer
		gzipWriter := gzip.NewWriter(&buf)
		start := time.Now()
		if _, err := gzipWriter.Write(request); err != nil {
			return err
		}
		if err := gzipWriter.Close(); err != nil {
			return err
		}
		GzipTotalTime += time.Since(start)
		requestCompressed := buf.Bytes()
		bodyReader = bytes.NewReader(requestCompressed)
	} else {
		bodyReader = bytes.NewReader(request)
	}
	if e.config.CalcZipRate {
		compressorStat.mergingTotal += bodyReader.Len()
		compressorStat.mergingNoGzipTotal += len(request)
		bzip, _ := CompressBZIP2(request)
		compressorStat.mergingBzipTotal += bzip
		lzma, _ := CompressLZMA(request)
		compressorStat.mergingLzmaTotal += lzma
		fmt.Printf("[Origin] %f KB\n", float32(compressorStat.originTotal)/1024)
		fmt.Printf("[NoGzipMerging] %f KB\n", float32(compressorStat.mergingNoGzipTotal)/1024)
		fmt.Printf("[LZMA] %f KB\n", float32(compressorStat.lzmaTotal)/1024)
		fmt.Printf("[BZIP] %f KB\n", float32(compressorStat.bzipTotal)/1024)
		fmt.Printf("[Gzip] %f KB\n", float32(compressorStat.gzipOnlyTotal)/1024)
		fmt.Printf("[MergingGZIP] %f KB\n", float32(compressorStat.mergingTotal)/1024)
		fmt.Printf("[MergingBZIP] %f KB\n", float32(compressorStat.mergingBzipTotal)/1024)
		fmt.Printf("[MergingLZMA] %f KB\n", float32(compressorStat.mergingLzmaTotal)/1024)
		fmt.Printf("[Now Dict Size] %d KB \n", DictSizeNow/1024)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bodyReader)
	if err != nil {
		return err
	}

	if e.config.EnableGzip || e.config.NoTraceZip {
		req.Header.Set("Content-Encoding", "gzip")
	}

	switch e.config.Encoding {
	case EncodingJSON:
		req.Header.Set("Content-Type", jsonContentType)
	case EncodingProto:
		req.Header.Set("Content-Type", protobufContentType)
	default:
		return fmt.Errorf("invalid encoding: %s", e.config.Encoding)
	}

	req.Header.Set("User-Agent", e.userAgent)

	resp, err := e.client.Do(req)
	if err != nil {
		needResetOrder = true
		return fmt.Errorf("failed to make an HTTP request: %w", err)
	}

	defer func() {
		// Discard any remaining response body when we are done reading.
		io.CopyN(io.Discard, resp.Body, maxHTTPResponseReadBytes) // nolint:errcheck
		resp.Body.Close()
	}()

	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		return handlePartialSuccessResponse(resp, partialSuccessHandler)
	}

	needResetOrder = true

	respStatus := readResponseStatus(resp)

	// Format the error message. Use the status if it is present in the response.
	var formattedErr error
	if respStatus != nil {
		formattedErr = fmt.Errorf(
			"error exporting items, request to %s responded with HTTP Status Code %d, Message=%s, Details=%v",
			url, resp.StatusCode, respStatus.Message, respStatus.Details)
	} else {
		formattedErr = fmt.Errorf(
			"error exporting items, request to %s responded with HTTP Status Code %d",
			url, resp.StatusCode)
	}

	if isRetryableStatusCode(resp.StatusCode) {
		// A retry duration of 0 seconds will trigger the default backoff policy
		// of our caller (retry handler).
		retryAfter := 0

		// Check if the server is overwhelmed.
		// See spec https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/otlp.md#otlphttp-throttling
		isThrottleError := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusServiceUnavailable
		if val := resp.Header.Get(headerRetryAfter); isThrottleError && val != "" {
			if seconds, err2 := strconv.Atoi(val); err2 == nil {
				retryAfter = seconds
			}
		}

		return exporterhelper.NewThrottleRetry(formattedErr, time.Duration(retryAfter)*time.Second)
	}

	return consumererror.NewPermanent(formattedErr)
}

// Determine if the status code is retryable according to the specification.
// For more, see https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/otlp.md#failures-1
func isRetryableStatusCode(code int) bool {
	switch code {
	case http.StatusTooManyRequests:
		return true
	case http.StatusBadGateway:
		return true
	case http.StatusServiceUnavailable:
		return true
	case http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func readResponseBody(resp *http.Response) ([]byte, error) {
	if resp.ContentLength == 0 {
		return nil, nil
	}

	maxRead := resp.ContentLength

	// if maxRead == -1, the ContentLength header has not been sent, so read up to
	// the maximum permitted body size. If it is larger than the permitted body
	// size, still try to read from the body in case the value is an error. If the
	// body is larger than the maximum size, proto unmarshaling will likely fail.
	if maxRead == -1 || maxRead > maxHTTPResponseReadBytes {
		maxRead = maxHTTPResponseReadBytes
	}
	protoBytes := make([]byte, maxRead)
	n, err := io.ReadFull(resp.Body, protoBytes)

	// No bytes read and an EOF error indicates there is no body to read.
	if n == 0 && (err == nil || errors.Is(err, io.EOF)) {
		return nil, nil
	}

	// io.ReadFull will return io.ErrorUnexpectedEOF if the Content-Length header
	// wasn't set, since we will try to read past the length of the body. If this
	// is the case, the body will still have the full message in it, so we want to
	// ignore the error and parse the message.
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}

	return protoBytes[:n], nil
}

// Read the response and decode the status.Status from the body.
// Returns nil if the response is empty or cannot be decoded.
func readResponseStatus(resp *http.Response) *status.Status {
	var respStatus *status.Status
	if resp.StatusCode >= 400 && resp.StatusCode <= 599 {
		// Request failed. Read the body. OTLP spec says:
		// "Response body for all HTTP 4xx and HTTP 5xx responses MUST be a
		// Protobuf-encoded Status message that describes the problem."
		respBytes, err := readResponseBody(resp)
		if err != nil {
			return nil
		}

		// Decode it as Status struct. See https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/otlp.md#failures
		respStatus = &status.Status{}
		err = proto.Unmarshal(respBytes, respStatus)
		if err != nil {
			return nil
		}
	}

	return respStatus
}

func handlePartialSuccessResponse(resp *http.Response, partialSuccessHandler partialSuccessHandler) error {
	bodyBytes, err := readResponseBody(resp)
	if err != nil {
		return err
	}

	return partialSuccessHandler(bodyBytes, resp.Header.Get("Content-Type"))
}

type partialSuccessHandler func(bytes []byte, contentType string) error

func (e *baseExporter) tracesPartialSuccessHandler(protoBytes []byte, contentType string) error {
	exportResponse := ptraceotlp.NewExportResponse()
	switch contentType {
	case protobufContentType:
		err := exportResponse.UnmarshalProto(protoBytes)
		if err != nil {
			return fmt.Errorf("error parsing protobuf response: %w", err)
		}
	case jsonContentType:
		err := exportResponse.UnmarshalJSON(protoBytes)
		if err != nil {
			return fmt.Errorf("error parsing json response: %w", err)
		}
	default:
		return nil
	}

	partialSuccess := exportResponse.PartialSuccess()
	if !(partialSuccess.ErrorMessage() == "" && partialSuccess.RejectedSpans() == 0) {
		e.logger.Warn("Partial success response",
			zap.String("message", exportResponse.PartialSuccess().ErrorMessage()),
			zap.Int64("dropped_spans", exportResponse.PartialSuccess().RejectedSpans()),
		)
	}
	return nil
}

func (e *baseExporter) metricsPartialSuccessHandler(protoBytes []byte, contentType string) error {
	exportResponse := pmetricotlp.NewExportResponse()
	switch contentType {
	case protobufContentType:
		err := exportResponse.UnmarshalProto(protoBytes)
		if err != nil {
			return fmt.Errorf("error parsing protobuf response: %w", err)
		}
	case jsonContentType:
		err := exportResponse.UnmarshalJSON(protoBytes)
		if err != nil {
			return fmt.Errorf("error parsing json response: %w", err)
		}
	default:
		return nil
	}

	partialSuccess := exportResponse.PartialSuccess()
	if !(partialSuccess.ErrorMessage() == "" && partialSuccess.RejectedDataPoints() == 0) {
		e.logger.Warn("Partial success response",
			zap.String("message", exportResponse.PartialSuccess().ErrorMessage()),
			zap.Int64("dropped_data_points", exportResponse.PartialSuccess().RejectedDataPoints()),
		)
	}
	return nil
}

func (e *baseExporter) logsPartialSuccessHandler(protoBytes []byte, contentType string) error {
	exportResponse := plogotlp.NewExportResponse()
	switch contentType {
	case protobufContentType:
		err := exportResponse.UnmarshalProto(protoBytes)
		if err != nil {
			return fmt.Errorf("error parsing protobuf response: %w", err)
		}
	case jsonContentType:
		err := exportResponse.UnmarshalJSON(protoBytes)
		if err != nil {
			return fmt.Errorf("error parsing json response: %w", err)
		}
	default:
		return nil
	}

	partialSuccess := exportResponse.PartialSuccess()
	if !(partialSuccess.ErrorMessage() == "" && partialSuccess.RejectedLogRecords() == 0) {
		e.logger.Warn("Partial success response",
			zap.String("message", exportResponse.PartialSuccess().ErrorMessage()),
			zap.Int64("dropped_log_records", exportResponse.PartialSuccess().RejectedLogRecords()),
		)
	}
	return nil
}
