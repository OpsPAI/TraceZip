package prefix_compressed_receiver // import "go.opentelemetry.io/collector/receiver/otlpreceiver"

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"time"

	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"angrychow/otel/prefix-compressed-receiver/internal/logs"
	"angrychow/otel/prefix-compressed-receiver/internal/metrics"
	"angrychow/otel/prefix-compressed-receiver/internal/trace"
)

var Dictionary = make(map[string]*CompressionDictionary, 0)

var DecompressionTotalTime time.Duration

var GzipDecompressionTotalTime time.Duration

var BodyLengthTotal uint64

type CompressionDictionary struct {
	AttributeNameDict  map[string]string
	AttributeValueDict map[string]string
	EventAttributeDict map[string]string
	EventNameDict      map[string]string
	PathDict           map[string][]string
	Orders             map[string][]string
	SpanNameDict       map[string]string
}

type UpdatesEntry struct {
	Key   string `json:"k"`
	Value string `json:"v"`
}

func (cd *CompressionDictionary) IncrementUpdate(datas []interface{}) error {

	// fmt.Println(datas)

	var updates [7][]UpdatesEntry

	for i := 0; i < 7; i++ {
		updates[i] = make([]UpdatesEntry, 0)
		t := datas[i].([]interface{})
		for _, item_ := range t {
			item := item_.(map[string]interface{})
			updates[i] = append(updates[i], UpdatesEntry{
				Key:   item["k"].(string),
				Value: item["v"].(string),
			})
		}
	}

	if len(updates[0]) > 0 {
		if cd.AttributeNameDict == nil {
			cd.AttributeNameDict = make(map[string]string)
		}
		for _, entry := range updates[0] {
			cd.AttributeNameDict[entry.Key] = entry.Value
		}
	}

	if len(updates[1]) > 0 {
		if cd.AttributeValueDict == nil {
			cd.AttributeValueDict = make(map[string]string)
		}
		for _, entry := range updates[1] {
			cd.AttributeValueDict[entry.Key] = entry.Value
		}
	}

	if len(updates[2]) > 0 {
		if cd.EventAttributeDict == nil {
			cd.EventAttributeDict = make(map[string]string)
		}
		for _, entry := range updates[2] {
			cd.EventAttributeDict[entry.Key] = entry.Value
		}
	}

	if len(updates[3]) > 0 {
		if cd.EventNameDict == nil {
			cd.EventNameDict = make(map[string]string)
		}
		for _, entry := range updates[3] {
			cd.EventNameDict[entry.Key] = entry.Value
		}
	}

	if len(updates[4]) > 0 {
		if cd.PathDict == nil {
			cd.PathDict = make(map[string][]string)
		}
		for _, entry := range updates[4] {
			var arr []string
			if err := json.Unmarshal([]byte(entry.Value), &arr); err != nil {
				return err
			}
			cd.PathDict[entry.Key] = arr
		}
	}

	if len(updates[5]) > 0 {
		if cd.SpanNameDict == nil {
			cd.SpanNameDict = make(map[string]string)
		}
		for _, entry := range updates[5] {
			cd.SpanNameDict[entry.Key] = entry.Value
		}
	}

	if len(updates[6]) > 0 {
		if cd.Orders == nil {
			cd.Orders = make(map[string][]string)
		}
		for _, entry := range updates[6] {
			// cd.Orders
			var order []string
			json.Unmarshal([]byte(entry.Value), &order)
			cd.Orders[entry.Key] = order
			fmt.Println(entry.Key)
			fmt.Println(order)
		}
	}
	return nil
}

func (cd *CompressionDictionary) FullUpdate(data []interface{}) error {
	// A temporary structure to map the JSON structure to the CompressionDictionary
	var temp []map[string]interface{} = make([]map[string]interface{}, 0)

	for _, item := range data {
		temp = append(temp, item.(map[string]interface{}))
	}

	// Initialize CompressionDictionary
	cd.AttributeNameDict = make(map[string]string)
	cd.AttributeValueDict = make(map[string]string)
	cd.EventAttributeDict = make(map[string]string)
	cd.EventNameDict = make(map[string]string)
	cd.Orders = make(map[string][]string)
	cd.PathDict = make(map[string][]string)
	cd.SpanNameDict = make(map[string]string)

	// Populate CompressionDictionary from the temporary structure
	if len(temp) > 0 {
		for k, v := range temp[0] {
			cd.AttributeNameDict[k] = v.(string)
		}
	}
	if len(temp) > 1 {
		for k, v := range temp[1] {
			cd.AttributeValueDict[k] = v.(string)
		}
	}
	if len(temp) > 2 {
		for k, v := range temp[2] {
			cd.EventAttributeDict[k] = v.(string)
		}
	}
	if len(temp) > 3 {
		for k, v := range temp[3] {
			cd.EventNameDict[k] = v.(string)
		}
	}
	if len(temp) > 4 {
		for k, v := range temp[4] {
			paths := make([]string, 0)
			for _, path := range v.([]interface{}) {
				paths = append(paths, path.(string))
			}
			cd.PathDict[k] = paths
		}
	}
	if len(temp) > 5 {
		for k, v := range temp[5] {
			var orders []string
			for _, order := range v.([]interface{}) {
				orders = append(orders, order.(string))
			}
			cd.Orders[k] = orders
		}
	}
	if len(temp) > 6 {
		for k, v := range temp[6] {
			cd.SpanNameDict[k] = v.(string)
		}
	}
	return nil
}

var fieldMap = map[string]string{
	"0": "trace_id",
	"1": "span_id",
	"2": "parent_span_id",
	"3": "flags",
	"4": "name",
	"5": "start_time_unix_nano",
	"6": "end_time_unix_nano",
	"7": "attributes",
	"8": "status",
	"9": "trace_state",
	"a": "links",
	"b": "dropped_attributes_count",
	"c": "dropped_events_count",
	"d": "dropped_links_count",
	"e": "events",
	"f": "kind",
}

// Pre-computed status with code=Internal to be used in case of a marshaling error.
var fallbackMsg = []byte(`{"code": 13, "message": "failed to marshal error message"}`)

const fallbackContentType = "application/json"

func handleTraces(resp http.ResponseWriter, req *http.Request, tracesReceiver *trace.Receiver, exportSpans string, NoTraceZip bool) {
	enc, ok := readContentType(resp, req)
	if NoTraceZip {
		var buf bytes.Buffer
		_, err := io.Copy(&buf, req.Body)
		if err != nil {
			http.Error(resp, "Failed to read request body", http.StatusInternalServerError)
			return
		}

		otlpReq, err := enc.unmarshalTracesRequest(buf.Bytes())

		if err != nil {
			writeError(resp, enc, err, http.StatusBadRequest)
			return
		}

		otlpResp, err := tracesReceiver.Export(req.Context(), otlpReq)
		if err != nil {
			writeError(resp, enc, err, http.StatusInternalServerError)
			return
		}

		msg, err := enc.marshalTracesResponse(otlpResp)
		if err != nil {
			writeError(resp, enc, err, http.StatusInternalServerError)
			return
		}
		writeResponse(resp, enc.contentType(), http.StatusOK, msg)
		return
	}
	mu.Lock()
	defer mu.Unlock()
	var err error
	if !ok {
		return
	}
	var body []byte
	var body_ map[string]interface{}
	if req.Header.Get("Content-Encoding") == "gzip" {
		var buf bytes.Buffer
		_, err = io.Copy(&buf, req.Body)
		if err != nil {
			http.Error(resp, "Failed to read request body", http.StatusInternalServerError)
			return
		}
		defer req.Body.Close()

		gz, err := gzip.NewReader(&buf)
		if err != nil {
			http.Error(resp, "Failed to create gzip reader", http.StatusInternalServerError)
			return
		}
		defer gz.Close()

		body, err = io.ReadAll(gz)
		if err != nil {
			http.Error(resp, "Failed to read gzip body", http.StatusInternalServerError)
			return
		}
	} else {
		body, err = io.ReadAll(req.Body)
		if err != nil {
			http.Error(resp, "Failed to read request body", http.StatusInternalServerError)
			return
		}
		defer req.Body.Close()
	}
	if !ok {
		writeError(resp, enc, err, http.StatusBadRequest)
		return
	}
	err = json.Unmarshal(body, &body_)
	if err != nil {
		writeError(resp, enc, err, http.StatusBadRequest)
		return
	}
	dict := Dictionary[body_["_"].(string)]
	resourcesSpans := body_["a"].([]interface{})
	for _, resourcesSpan := range resourcesSpans {
		resource := resourcesSpan.(map[string]interface{})["resource"].(map[string]interface{})["attributes"].([]interface{})
		for _, item := range resource {
			d_ := make(map[string]interface{})
			json.Unmarshal([]byte(item.(map[string]interface{})["value"].(string)), &d_)
			item.(map[string]interface{})["value"] = d_
		}
		for _, scopeSpan := range resourcesSpan.(map[string]interface{})["scopeSpans"].([]interface{}) {
			var minTime float64 = scopeSpan.(map[string]interface{})["to"].(float64)
			var minEvtTime float64 = scopeSpan.(map[string]interface{})["eo"].(float64)
			delete(scopeSpan.(map[string]interface{}), "to")
			delete(scopeSpan.(map[string]interface{}), "eo")
			for _, span_ := range scopeSpan.(map[string]interface{})["spans"].([]interface{}) {
				span := span_.(map[string]interface{})
				for k, v := range fieldMap {
					if span[k] == nil {
						continue
					}
					span[v] = span[k]
					delete(span, k)
				}
				name__ := span["name"].(string)
				span["name"] = dict.SpanNameDict[name__]
				var pathId string
				var pathArray []string
				if span["_"] != nil {
					pathId = span["_"].(string)
					pathArray = dict.PathDict[pathId]
					if pathArray == nil {
						// diff sync system failed. we brutally cracked here, let exporter re-constructing SRT.
						panic("no such pathId " + pathId)
					}
				} else {
					pathArray = []string{}
				}

				Order := dict.Orders[span["name"].(string)]
				span["start_time_unix_nano"] = span["start_time_unix_nano"].(float64) + minTime
				span["end_time_unix_nano"] = span["end_time_unix_nano"].(float64) + minTime
				// fmt.Println(span["attributes"])
				for _, item_ := range span["attributes"].([]interface{}) {
					item := item_.(map[string]interface{})
					item["key"] = dict.AttributeNameDict[item["k"].(string)]
					item["value"] = item["v"]
					delete(item, "k")
					delete(item, "v")
				}
				if len(pathArray) != len(Order) {
					orderString, _ := json.Marshal(Order)
					pathString, _ := json.Marshal(pathArray)
					// diff sync system failed. we brutally cracked here, let exporter re-constructing SRT.
					panic("SRT Dictionary Failed." + string(orderString) + "," + string(pathString))
				}
				for index, attr := range Order {
					var valueParse interface{}
					if pathArray[index] == "#" {
						continue
					}
					json.Unmarshal([]byte(dict.AttributeValueDict[pathArray[index]]), &valueParse)
					span["attributes"] = append(span["attributes"].([]interface{}), map[string]interface{}{
						"key":   dict.AttributeNameDict[attr],
						"value": valueParse,
					})
				}
				if span["events"] != nil {
					for _, event_ := range span["events"].([]interface{}) {
						event := event_.(map[string]interface{})
						event["time_unix_nano"] = event["t"].(float64) + minEvtTime
						delete(event, "t")
						event["name"] = dict.EventNameDict[event["n"].(string)]
						delete(event, "n")
						if event["d"] != nil {
							event["dropped_attributes_count"] = event["d"].(int)
							delete(event, "d")
						}
						if event["a"] != nil {
							var value_ interface{}
							json.Unmarshal([]byte(dict.EventAttributeDict[event["a"].(string)]), &value_)
							for _, attr := range value_.([]interface{}) {
								m_ := make(map[string]interface{})
								json.Unmarshal([]byte(attr.(map[string]interface{})["value"].(string)), &m_)
								attr.(map[string]interface{})["value"] = m_
							}
							event["attributes"] = value_
							delete(event, "a")
						}

					}
				}
				delete(span, "_")
			}
		}
	}

	body, err = json.Marshal(map[string]interface{}{
		"resource_spans": resourcesSpans,
	})

	if err != nil {
		writeError(resp, enc, err, http.StatusBadRequest)
		return
	}

	if exportSpans != "" {
		go sendPostRequest(exportSpans, body)
	}

	otlpReq, err := enc.unmarshalTracesRequest(body)

	if err != nil {
		writeError(resp, enc, err, http.StatusBadRequest)
		return
	}

	otlpResp, err := tracesReceiver.Export(req.Context(), otlpReq)
	if err != nil {
		writeError(resp, enc, err, http.StatusInternalServerError)
		return
	}

	msg, err := enc.marshalTracesResponse(otlpResp)
	if err != nil {
		writeError(resp, enc, err, http.StatusInternalServerError)
		return
	}
	writeResponse(resp, enc.contentType(), http.StatusOK, msg)
}

func handleMetrics(resp http.ResponseWriter, req *http.Request, metricsReceiver *metrics.Receiver) {
	enc, ok := readContentType(resp, req)
	if !ok {
		return
	}

	body, ok := readAndCloseBody(resp, req, enc)
	if !ok {
		return
	}

	otlpReq, err := enc.unmarshalMetricsRequest(body)
	if err != nil {
		writeError(resp, enc, err, http.StatusBadRequest)
		return
	}

	otlpResp, err := metricsReceiver.Export(req.Context(), otlpReq)
	if err != nil {
		writeError(resp, enc, err, http.StatusInternalServerError)
		return
	}

	msg, err := enc.marshalMetricsResponse(otlpResp)
	if err != nil {
		writeError(resp, enc, err, http.StatusInternalServerError)
		return
	}
	writeResponse(resp, enc.contentType(), http.StatusOK, msg)
}

func handleLogs(resp http.ResponseWriter, req *http.Request, logsReceiver *logs.Receiver) {
	enc, ok := readContentType(resp, req)
	if !ok {
		return
	}

	body, ok := readAndCloseBody(resp, req, enc)
	if !ok {
		return
	}

	otlpReq, err := enc.unmarshalLogsRequest(body)
	if err != nil {
		writeError(resp, enc, err, http.StatusBadRequest)
		return
	}

	otlpResp, err := logsReceiver.Export(req.Context(), otlpReq)
	if err != nil {
		writeError(resp, enc, err, http.StatusInternalServerError)
		return
	}

	msg, err := enc.marshalLogsResponse(otlpResp)
	if err != nil {
		writeError(resp, enc, err, http.StatusInternalServerError)
		return
	}
	writeResponse(resp, enc.contentType(), http.StatusOK, msg)
}

func readContentType(resp http.ResponseWriter, req *http.Request) (encoder, bool) {
	if req.Method != http.MethodPost {
		handleUnmatchedMethod(resp)
		return nil, false
	}

	switch getMimeTypeFromContentType(req.Header.Get("Content-Type")) {
	case pbContentType:
		return pbEncoder, true
	case jsonContentType:
		return jsEncoder, true
	default:
		handleUnmatchedContentType(resp)
		return nil, false
	}
}

func readAndCloseBody(resp http.ResponseWriter, req *http.Request, enc encoder) ([]byte, bool) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		writeError(resp, enc, err, http.StatusBadRequest)
		return nil, false
	}
	if err = req.Body.Close(); err != nil {
		writeError(resp, enc, err, http.StatusBadRequest)
		return nil, false
	}
	return body, true
}

// writeError encodes the HTTP error inside a rpc.Status message as required by the OTLP protocol.
func writeError(w http.ResponseWriter, encoder encoder, err error, statusCode int) {
	s, ok := status.FromError(err)
	if !ok {
		s = errorMsgToStatus(err.Error(), statusCode)
	}
	writeStatusResponse(w, encoder, statusCode, s.Proto())
}

// errorHandler encodes the HTTP error message inside a rpc.Status message as required
// by the OTLP protocol.
func errorHandler(w http.ResponseWriter, r *http.Request, errMsg string, statusCode int) {
	s := errorMsgToStatus(errMsg, statusCode)
	switch getMimeTypeFromContentType(r.Header.Get("Content-Type")) {
	case pbContentType:
		writeStatusResponse(w, pbEncoder, statusCode, s.Proto())
		return
	case jsonContentType:
		writeStatusResponse(w, jsEncoder, statusCode, s.Proto())
		return
	}
	writeResponse(w, fallbackContentType, http.StatusInternalServerError, fallbackMsg)
}

func writeStatusResponse(w http.ResponseWriter, enc encoder, statusCode int, rsp *spb.Status) {
	msg, err := enc.marshalStatus(rsp)
	if err != nil {
		writeResponse(w, fallbackContentType, http.StatusInternalServerError, fallbackMsg)
		return
	}

	writeResponse(w, enc.contentType(), statusCode, msg)
}

func writeResponse(w http.ResponseWriter, contentType string, statusCode int, msg []byte) {
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(statusCode)
	// Nothing we can do with the error if we cannot write to the response.
	_, _ = w.Write(msg)
}

func errorMsgToStatus(errMsg string, statusCode int) *status.Status {
	if statusCode == http.StatusBadRequest {
		return status.New(codes.InvalidArgument, errMsg)
	}
	return status.New(codes.Unknown, errMsg)
}

func getMimeTypeFromContentType(contentType string) string {
	mediatype, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return ""
	}
	return mediatype
}

func handleUnmatchedMethod(resp http.ResponseWriter) {
	status := http.StatusMethodNotAllowed
	writeResponse(resp, "text/plain", status, []byte(fmt.Sprintf("%v method not allowed, supported: [POST]", status)))
}

func handleUnmatchedContentType(resp http.ResponseWriter) {
	status := http.StatusUnsupportedMediaType
	writeResponse(resp, "text/plain", status, []byte(fmt.Sprintf("%v unsupported media type, supported: [%s, %s]", status, jsonContentType, pbContentType)))
}

func handleTracesDictionary(resp http.ResponseWriter, req *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	_, ok := readContentType(resp, req)
	if !ok {
		return
	}
	var err error
	var body []byte

	if req.Header.Get("Content-Encoding") == "gzip" {
		var buf bytes.Buffer
		_, err = io.Copy(&buf, req.Body)
		if err != nil {
			http.Error(resp, "Failed to read request body", http.StatusInternalServerError)
			return
		}
		defer req.Body.Close()

		gz, err := gzip.NewReader(&buf)
		if err != nil {
			http.Error(resp, "Failed to create gzip reader", http.StatusInternalServerError)
			return
		}
		defer gz.Close()

		body, err = io.ReadAll(gz)
		if err != nil {
			http.Error(resp, "Failed to read gzip body", http.StatusInternalServerError)
			return
		}
	} else {
		body, err = io.ReadAll(req.Body)
		if err != nil {
			http.Error(resp, "Failed to read request body", http.StatusInternalServerError)
			return
		}
		defer req.Body.Close()
	}
	body_ := make(map[string]interface{})
	json.Unmarshal(body, &body_)
	if Dictionary[body_["_"].(string)] == nil {
		Dictionary[body_["_"].(string)] = &CompressionDictionary{}
	}
	if body_["t"].(string) == "a" {
		Dictionary[body_["_"].(string)].FullUpdate(body_["n"].([]interface{}))
	} else if body_["t"].(string) == "i" {
		Dictionary[body_["_"].(string)].IncrementUpdate(body_["n"].([]interface{}))
	}
	writeResponse(resp, "text/plain", http.StatusOK, []byte(`receive package`))
}

func sendPostRequest(url string, body []byte) {
	if url == "" {
		return
	}
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		fmt.Println("Error creating request:", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error sending request:", err)
		return
	}
	defer resp.Body.Close()

	fmt.Println("Response status:", resp.Status)
}
