// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package ptraceotlp // import "go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"github.com/google/uuid"
	"go.opentelemetry.io/collector/pdata/internal"
	otlpcollectortrace "go.opentelemetry.io/collector/pdata/internal/data/protogen/collector/trace/v1"
	v1_resource "go.opentelemetry.io/collector/pdata/internal/data/protogen/resource/v1"
	v1_trace "go.opentelemetry.io/collector/pdata/internal/data/protogen/trace/v1"
	tele_json "go.opentelemetry.io/collector/pdata/internal/json"
	"go.opentelemetry.io/collector/pdata/internal/otlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const NO_PATH_EXIST = "NO_PATH_EXIST"

func (ms ExportRequest) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	if err := tele_json.Marshal(&buf, ms.orig); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (ms ExportRequest) MarshalJSON__(deleteResource bool) ([]byte, error) {
	var buf bytes.Buffer
	// delete Resource Attributes, which contains personal message
	if deleteResource {
		for _, resourceSpan := range ms.orig.ResourceSpans {
			resourceSpan.Resource = v1_resource.Resource{}
		}
	}
	if err := tele_json.Marshal(&buf, ms.orig); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type Resource__ struct {
	// Set of attributes that describe the resource.
	// Attribute keys MUST be unique (it is not allowed to have more than one
	// attribute with the same key).
	Attributes []Attributes__ `json:"attributes"`
	// dropped_attributes_count is the number of dropped attributes. If the value is 0, then
	// no attributes were dropped.
	DroppedAttributesCount uint32 `json:"dropped_attributes_count,omitempty"`
}

var jsonUnmarshaler = &ptrace.JSONUnmarshaler{}
var attrNameMap = make(map[string]string)
var attrNameDict = make(map[string]string)
var dictCounter = 0

var updateAttrValueDict = make([]UpdatesEntry, 0)
var updateAttrNameDict = make([]UpdatesEntry, 0)
var updateEventNameDict = make([]UpdatesEntry, 0)
var updateEventAttrDict = make([]UpdatesEntry, 0)
var updatePathDict = make([]UpdatesEntry, 0)
var updateSpanNameDict = make([]UpdatesEntry, 0)
var updateOrders = make([]UpdatesEntry, 0)

// ExportRequest represents the request for gRPC/HTTP client/server.
// It's a wrapper for ptrace.Traces data.
type ExportRequest struct {
	orig  *otlpcollectortrace.ExportTraceServiceRequest
	state *internal.State
}

// NewExportRequest returns an empty ExportRequest.
func NewExportRequest() ExportRequest {
	state := internal.StateMutable
	return ExportRequest{
		orig:  &otlpcollectortrace.ExportTraceServiceRequest{},
		state: &state,
	}
}

// NewExportRequestFromTraces returns a ExportRequest from ptrace.Traces.
// Because ExportRequest is a wrapper for ptrace.Traces,
// any changes to the provided Traces struct will be reflected in the ExportRequest and vice versa.
func NewExportRequestFromTraces(td ptrace.Traces) ExportRequest {
	return ExportRequest{
		orig:  internal.GetOrigTraces(internal.Traces(td)),
		state: internal.GetTracesState(internal.Traces(td)),
	}
}

// MarshalProto marshals ExportRequest into proto bytes.
func (ms ExportRequest) MarshalProto() ([]byte, error) {
	return ms.orig.Marshal()
}

// UnmarshalProto unmarshalls ExportRequest from proto bytes.
func (ms ExportRequest) UnmarshalProto(data []byte) error {
	if err := ms.orig.Unmarshal(data); err != nil {
		return err
	}
	otlp.MigrateTraces(ms.orig.ResourceSpans)
	return nil
}

type ScopeSpan struct {
	SchemaUrl  string        `json:"schemaUrl,omitempty"`
	Scope      interface{}   `json:"scope,omitempty"`
	OffsetMain uint64        `json:"to"`
	EOffset    uint64        `json:"eo,omitempty"`
	Spans      []interface{} `json:"spans,omitempty"`
}

type ExportData struct {
	SchemaUrl  string      `json:"schemaUrl,omitempty"`
	Resource   Resource__  `json:"resource,omitempty"`
	ScopeSpans []ScopeSpan `json:"scopeSpans,omitempty"`
}

type UpdatesEntry struct {
	Key   string `json:"k"`
	Value string `json:"v"`
}

type Attributes__ struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

type SpanAttrSort struct {
	AttrName string
	Times    int
}

type SpanRetrieveTrieBranch struct {
	AttrHash   string
	NextBranch map[string]*SpanRetrieveTrieBranch
	NextLeaf   map[string]*SpanRetrieveTrieLeaf
}

type SpanRetrieveTrieLeaf struct {
	PathHash string
}

var rootSRT *SpanRetrieveTrieBranch

// ByTimes implements sort.Interface for []SpanAttrSort based on
// the Times field.
type ByTimes []SpanAttrSort

func (a ByTimes) Len() int           { return len(a) }
func (a ByTimes) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByTimes) Less(i, j int) bool { return a[i].Times < a[j].Times }

type SpanEvent struct {
	EventName              string `json:"n"`
	Time                   uint64 `json:"t,omitempty"`
	DroppedAttributesCount uint32 `json:"d,omitempty"`
	Attributes             string `json:"a,omitempty"`
}

var PathCount = 0

var HashSpanName = make(map[string]string)

var SpanNameDict = make(map[string]string)

var SpanNameCount = 0

// path number to []string
var PathDict = make(map[string][]string)

var HashEventAttributes = make(map[string]string)

var HashEventName = make(map[string]string)

var EventAttributesDict = make(map[string]string)

var EventNameDict = make(map[string]string)

var EventAttributesCnt = 0

var EventNameCnt = 0

/*
	The following variable is all about Event Attribute Compression
*/

const asciiChars string = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

func Number2String(number int) string {
	ret := ""
	for {
		bytes := []byte{asciiChars[number%62]}
		ret = ret + string(bytes)
		number /= 62
		if number == 0 {
			break
		}
	}
	return ret
}

var spansBuffer = make([]v1_trace.Span, 0)

// [attr value], used to map attrvalue to a short one
var spansAttrValueHash = make(map[string]string)

var spansAttrValueDict = make(map[string]string)

var spansAttrValueCnt = 0

// [span name][attr name][attr value], used to know how many attrvalue in buffer
var spansAttrValueCount = make(map[string]map[string]map[string]int)

// [span name][attr name][attr value], used to know whether a attrvalue is recorded
var spansAttrValueExists = make(map[string]map[string]map[string]bool)

// [span name][attr name], used to know how many kinds of attrvalue in buffer
var spansAttrValueOptCount = make(map[string]map[string]int)

// var needUpdateAttrValueDict = false

// var needUpdateAttrNameDict = false

// var needUpdatePathDict = false

// var needUpdateEventNameDict = false

// var needUpdateEventAttributesDict = false

var sendDictFull = true

var orders = make(map[string][]string)
var ordersZip = make(map[string][]string)
var ordersMap = make(map[string]map[string]bool)

var mu sync.Mutex

var dictionaryUuid string = ""

func setAllOrders(limited int) {
	rootSRT = &SpanRetrieveTrieBranch{
		NextBranch: make(map[string]*SpanRetrieveTrieBranch),
	}
	fmt.Println("Orders Change!!!")
	orders = make(map[string][]string)
	PathDict = make(map[string][]string)
	spansAttrValueHash = make(map[string]string)
	spansAttrValueDict = make(map[string]string)
	spansAttrValueCnt = 0
	PathCount = 0
	for spanName := range spansAttrValueOptCount {
		rootSRT.NextBranch[spanName] = &SpanRetrieveTrieBranch{}
		tobeSorted := make(ByTimes, 0)
		for attrName, count := range spansAttrValueOptCount[spanName] {
			tobeSorted = append(tobeSorted, SpanAttrSort{
				AttrName: attrName,
				Times:    count,
			})
		}
		sort.Sort(tobeSorted)
		orders[spanName] = make([]string, 0)
		ordersZip[spanName] = make([]string, 0)
		ordersMap[spanName] = make(map[string]bool)
		for _, order := range tobeSorted {
			if order.Times > limited {
				break
			}
			orders[spanName] = append(orders[spanName], order.AttrName)
			ordersZip[spanName] = append(ordersZip[spanName], attrNameMap[order.AttrName])
			ordersMap[spanName][order.AttrName] = true
		}
		// fmt.Println(orders[spanName])
		// fmt.Println(spanName)
		// fmt.Println(ordersZip)
	}
}

func setOrder(limited int, spanNames []string) {
	for _, spanName := range spanNames {
		rootSRT.NextBranch[spanName] = &SpanRetrieveTrieBranch{}
		tobeSorted := make(ByTimes, 0)
		for attrName, count := range spansAttrValueOptCount[spanName] {
			tobeSorted = append(tobeSorted, SpanAttrSort{
				AttrName: attrName,
				Times:    count,
			})
		}
		sort.Sort(tobeSorted)
		orders[spanName] = make([]string, 0)
		ordersZip[spanName] = make([]string, 0)
		ordersMap[spanName] = make(map[string]bool)
		for _, order := range tobeSorted {
			if order.Times > limited {
				break
			}
			orders[spanName] = append(orders[spanName], order.AttrName)
			ordersZip[spanName] = append(ordersZip[spanName], attrNameMap[order.AttrName])
			ordersMap[spanName][order.AttrName] = true
		}
		val_, _ := json.Marshal(ordersZip[spanName])
		fmt.Println(spanName)
		fmt.Println(ordersZip[spanName])
		fmt.Println(orders[spanName])
		updateOrders = append(updateOrders, UpdatesEntry{
			Key:   spanName,
			Value: string(val_),
		})
	}
}

func RetrieveSRT(node *SpanRetrieveTrieBranch, pathArray []string, depth int) (string, bool) {
	if len(pathArray) == 0 {
		return NO_PATH_EXIST, false
	}
	nowAttr := pathArray[depth]
	if len(pathArray)-1 == depth {
		update_ := false
		if node.NextLeaf == nil {
			node.NextLeaf = make(map[string]*SpanRetrieveTrieLeaf)
		}
		if node.NextLeaf[nowAttr] == nil {
			update_ = true
			pathHash := Number2String(PathCount)
			PathCount++
			PathDict[pathHash] = pathArray
			data_, _ := json.Marshal(pathArray)
			updatePathDict = append(updatePathDict, UpdatesEntry{
				Key:   pathHash,
				Value: string(data_),
			})
			node.NextLeaf[nowAttr] = &SpanRetrieveTrieLeaf{PathHash: pathHash}
		}
		return node.NextLeaf[nowAttr].PathHash, update_
	}
	if node == nil {
		panic("SRT Damage, Check Function RetrieveSTR@pdata/ptrace/ptraceotlp/request.go")
	}
	if node.NextBranch == nil {
		node.NextBranch = make(map[string]*SpanRetrieveTrieBranch)
	}
	if node.NextBranch[nowAttr] == nil {
		node.NextBranch[nowAttr] = &SpanRetrieveTrieBranch{
			AttrHash: nowAttr,
		}
	}
	return RetrieveSRT(node.NextBranch[nowAttr], pathArray, depth+1)
}

// MarshalJSON marshals ExportRequest into JSON bytes. In TraceZip, we compress spans here too.
func (ms ExportRequest) MarshalWithTraceZip(BufferSize int, AttrLimited int, ThresholdRate int, ExplictReset bool, DeleteResource bool) (string, []interface{}, []interface{}, interface{}) {
	mu.Lock()
	defer mu.Unlock()

	if dictionaryUuid == "" {
		dictionaryUuid = uuid.NewString()
	}

	var needUpdate = false

	var emergeNewSpanName = make([]string, 0)

	var minTime uint64 = 1 << 63
	var minEvtTime uint64 = 1 << 53

	if ExplictReset {
		sendDictFull = true
	}

	// Update Buffer
	for _, resourcesSpans := range ms.orig.ResourceSpans {
		for _, scopeSpans := range resourcesSpans.ScopeSpans {
			for _, span := range scopeSpans.Spans {
				if span.StartTimeUnixNano < minTime {
					minTime = span.StartTimeUnixNano
				}
				if span.Events != nil {
					for _, event := range span.Events {
						if event.TimeUnixNano < minEvtTime {
							minEvtTime = event.TimeUnixNano
						}
					}
				}
				if len(spansBuffer) > BufferSize {
					expired := spansBuffer[0]
					for _, attribute := range expired.Attributes {

						value_, _ := attribute.Value.Marshal()
						value := string(value_)
						if spansAttrValueCount[expired.Name][attribute.Key][value]--; spansAttrValueCount[expired.Name][attribute.Key][value] == 0 {
							spansAttrValueOptCount[expired.Name][attribute.Key]--
							if spansAttrValueOptCount[expired.Name][attribute.Key]--; spansAttrValueOptCount[expired.Name][attribute.Key] < AttrLimited && !ordersMap[expired.Name][attribute.Key] {
								needUpdate = true
								setOrder(AttrLimited, []string{expired.Name})
							}
							spansAttrValueExists[expired.Name][attribute.Key][value] = false
						}
					}
					spansBuffer = spansBuffer[1:]
				}
				spansBuffer = append(spansBuffer, *span)
				if spansAttrValueOptCount[span.Name] == nil {
					emergeNewSpanName = append(emergeNewSpanName, span.Name)
					HashSpanName[span.Name] = Number2String(SpanNameCount)
					SpanNameDict[Number2String(SpanNameCount)] = span.Name
					updateSpanNameDict = append(updateSpanNameDict, UpdatesEntry{
						Key:   Number2String(SpanNameCount),
						Value: span.Name,
					})
					SpanNameCount++
					needUpdate = true
					spansAttrValueOptCount[span.Name] = make(map[string]int)
					spansAttrValueCount[span.Name] = make(map[string]map[string]int)
					spansAttrValueExists[span.Name] = make(map[string]map[string]bool)
				}
				for _, attribute := range span.Attributes {
					value_, _ := attribute.Value.Marshal()
					value := string(value_)
					if len(attrNameMap[attribute.Key]) == 0 {
						needUpdate = true
						attrNameMap[attribute.Key] = Number2String(dictCounter)
						attrNameDict[Number2String(dictCounter)] = attribute.Key
						updateAttrNameDict = append(updateAttrNameDict, UpdatesEntry{
							Key:   attrNameMap[attribute.Key],
							Value: attribute.Key,
						})
						dictCounter++
					}
					if spansAttrValueOptCount[span.Name][attribute.Key] == 0 {
						spansAttrValueCount[span.Name][attribute.Key] = make(map[string]int)
						spansAttrValueExists[span.Name][attribute.Key] = make(map[string]bool)
					}
					if !spansAttrValueExists[span.Name][attribute.Key][value] {
						spansAttrValueExists[span.Name][attribute.Key][value] = true
						spansAttrValueOptCount[span.Name][attribute.Key]++
					}
					spansAttrValueCount[span.Name][attribute.Key][value]++
				}
			}
		}
	}
	if sendDictFull {
		needUpdate = true
		setAllOrders(AttrLimited)
	} else if len(emergeNewSpanName) != 0 {
		needUpdate = true
		setOrder(AttrLimited, emergeNewSpanName)
	}
	export := make([]ExportData, 0)
	for _, resourcesSpan := range ms.orig.ResourceSpans {
		// resourcesSpan.Resource = {}
		resourcesSpan_ := ExportData{}
		// resourcesSpan_.Resource = resourcesSpan.Resource
		attrs := make([]Attributes__, 0)

		if resourcesSpan.Resource.Attributes != nil && !DeleteResource {
			for _, attr := range resourcesSpan.Resource.Attributes {
				value_, _ := attr.Value.Marshal()
				attrs = append(attrs, Attributes__{
					Key:   attr.Key,
					Value: value_,
				})
			}
		}

		resourcesSpan_.Resource = Resource__{
			DroppedAttributesCount: resourcesSpan.Resource.DroppedAttributesCount,
			Attributes:             attrs,
		}
		resourcesSpan_.SchemaUrl = resourcesSpan.SchemaUrl
		scopeSpans_ := make([]ScopeSpan, 0)
		for _, scopeSpan := range resourcesSpan.ScopeSpans {
			scopeSpan_ := ScopeSpan{}
			scopeSpan_.SchemaUrl = scopeSpan.SchemaUrl
			scopeSpan_.Scope = scopeSpan.Scope
			scopeSpan_.OffsetMain = minTime
			scopeSpan_.EOffset = minEvtTime
			spans := make([]interface{}, 0)
			for _, span := range scopeSpan.Spans {
				pathArray := make([]string, 0)
				span.StartTimeUnixNano = span.StartTimeUnixNano - minTime
				span.EndTimeUnixNano = span.EndTimeUnixNano - minTime
				for _, order := range orders[span.Name] {
					found := false
					for _, attribute := range span.Attributes {
						if order != attribute.Key {
							continue
						}
						value_, _ := attribute.Value.Marshal()
						value := string(value_)
						if spansAttrValueHash[value] == "" {
							spansAttrValueHash[value] = Number2String(spansAttrValueCnt)
							spansAttrValueDict[Number2String(spansAttrValueCnt)] = value
							updateAttrValueDict = append(updateAttrValueDict, UpdatesEntry{
								Key:   Number2String(spansAttrValueCnt),
								Value: value,
							})
							spansAttrValueCnt++
							needUpdate = true
						}
						pathArray = append(pathArray, spansAttrValueHash[value])
						found = true
					}
					if !found {
						pathArray = append(pathArray, "#")
					}
				}
				var pathHash string
				var temp bool
				pathHash, temp = RetrieveSRT(rootSRT.NextBranch[span.Name], pathArray, 0)
				if temp {
					needUpdate = temp
				}
				span_ := make(map[string]interface{})
				if pathHash != NO_PATH_EXIST {
					span_["_"] = pathHash
				}
				span_["0"] = span.TraceId
				span_["1"] = span.SpanId
				span_["2"] = span.ParentSpanId
				span_["3"] = span.Flags
				span_["f"] = span.Kind
				span_["4"] = HashSpanName[span.Name]
				span_["5"] = span.StartTimeUnixNano
				span_["6"] = span.EndTimeUnixNano
				span_["7"] = make([]interface{}, 0) // attributes
				for _, attribute := range span.Attributes {
					if !ordersMap[span.Name][attribute.Key] {
						attribute_ := make(map[string]interface{})
						attribute_["k"] = attrNameMap[attribute.Key]
						d__ := make(map[string]interface{})
						attribute_["v"], _ = attribute.Value.Marshal()
						json.Unmarshal(attribute_["v"].([]byte), &d__)
						attribute_["v"] = d__
						span_["7"] = append(span_["7"].([]interface{}), attribute_)
					}
				}
				span_["8"] = span.Status
				if span.TraceState != "" {
					span_["9"] = span.TraceState
				}
				if span.Links != nil {
					span_["a"] = span.Links
				}
				if span.DroppedAttributesCount != 0 {
					span_["b"] = span.DroppedAttributesCount
				}
				if span.DroppedEventsCount != 0 {
					span_["c"] = span.DroppedEventsCount
				}
				if span.DroppedLinksCount != 0 {
					span_["d"] = span.DroppedLinksCount
				}
				if span.Events != nil {
					span_["e"] = make([]SpanEvent, 0)
					for _, event := range span.Events {
						event_ := SpanEvent{}
						if HashEventName[event.Name] == "" {
							HashEventName[event.Name] = Number2String(EventNameCnt)
							EventNameDict[Number2String(EventNameCnt)] = event.Name
							updateEventNameDict = append(updateEventNameDict, UpdatesEntry{
								Key:   Number2String(EventNameCnt),
								Value: event.Name,
							})
							needUpdate = true
							EventNameCnt++
						}
						event_.EventName = HashEventName[event.Name]
						event_.DroppedAttributesCount = event.DroppedAttributesCount
						event_.Time = event.TimeUnixNano - minEvtTime
						attrs_split := make([]map[string]interface{}, 0)
						for _, attr := range event.Attributes {
							value_, _ := attr.Value.Marshal()
							value__ := string(value_)
							attrs_split = append(attrs_split, map[string]interface{}{
								"key":   attr.Key,
								"value": value__,
							})
						}
						attrs_, _ := json.Marshal(attrs_split)
						attrs := string(attrs_)
						if HashEventAttributes[attrs] == "" {
							HashEventAttributes[attrs] = Number2String(EventAttributesCnt)
							EventAttributesDict[Number2String(EventAttributesCnt)] = attrs
							updateEventAttrDict = append(updateEventAttrDict, UpdatesEntry{
								Key:   Number2String(EventAttributesCnt),
								Value: attrs,
							})
							EventAttributesCnt++
							needUpdate = true
						}
						event_.Attributes = HashEventAttributes[attrs]
						span_["e"] = append(span_["e"].([]SpanEvent), event_)
					}
				}
				spans = append(spans, span_)
			}
			scopeSpan_.Spans = spans
			scopeSpans_ = append(scopeSpans_, scopeSpan_)
		}
		resourcesSpan_.ScopeSpans = scopeSpans_
		export = append(export, resourcesSpan_)
	}

	var incrementUpdate []interface{} = nil
	var fullUpdate []interface{} = nil
	if sendDictFull {
		fullUpdate = make([]interface{}, 0)
		fullUpdate = append(fullUpdate, attrNameDict)
		fullUpdate = append(fullUpdate, spansAttrValueDict)
		fullUpdate = append(fullUpdate, EventAttributesDict)
		fullUpdate = append(fullUpdate, EventNameDict)
		fullUpdate = append(fullUpdate, PathDict)
		fullUpdate = append(fullUpdate, ordersZip)
		fullUpdate = append(fullUpdate, SpanNameDict)
		// fmt.Println(EventAttributesDict)
		sendDictFull = false
		needUpdate = false
	} else if needUpdate {
		incrementUpdate = make([]interface{}, 0)
		incrementUpdate = append(incrementUpdate, updateAttrNameDict)
		incrementUpdate = append(incrementUpdate, updateAttrValueDict)
		incrementUpdate = append(incrementUpdate, updateEventAttrDict)
		incrementUpdate = append(incrementUpdate, updateEventNameDict)
		incrementUpdate = append(incrementUpdate, updatePathDict)
		incrementUpdate = append(incrementUpdate, updateSpanNameDict)
		incrementUpdate = append(incrementUpdate, updateOrders)
		needUpdate = false
		updateAttrValueDict = make([]UpdatesEntry, 0)
		updateAttrNameDict = make([]UpdatesEntry, 0)
		updateEventNameDict = make([]UpdatesEntry, 0)
		updateEventAttrDict = make([]UpdatesEntry, 0)
		updatePathDict = make([]UpdatesEntry, 0)
		updateSpanNameDict = make([]UpdatesEntry, 0)
		updateOrders = make([]UpdatesEntry, 0)
	}

	if PathCount > ThresholdRate {
		sendDictFull = true
	}

	return dictionaryUuid, fullUpdate, incrementUpdate, export
}

// UnmarshalJSON unmarshalls ExportRequest from JSON bytes.
func (ms ExportRequest) UnmarshalJSON(data []byte) error {
	td, err := jsonUnmarshaler.UnmarshalTraces(data)
	if err != nil {
		return err
	}
	*ms.orig = *internal.GetOrigTraces(internal.Traces(td))
	return nil
}

func (ms ExportRequest) Traces() ptrace.Traces {
	return ptrace.Traces(internal.NewTraces(ms.orig, ms.state))
}

func (ms ExportRequest) SendFull() []interface{} {
	fullUpdate := make([]interface{}, 0)
	fullUpdate = append(fullUpdate, attrNameDict)
	fullUpdate = append(fullUpdate, spansAttrValueDict)
	fullUpdate = append(fullUpdate, EventAttributesDict)
	fullUpdate = append(fullUpdate, EventNameDict)
	fullUpdate = append(fullUpdate, PathDict)
	fullUpdate = append(fullUpdate, ordersZip)
	fullUpdate = append(fullUpdate, SpanNameDict)
	return fullUpdate
}
