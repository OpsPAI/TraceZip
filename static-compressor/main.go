package main

import (
	byteslib "bytes"
	"container/heap"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func printMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

type HuffmanDictionary struct {
	BitNumber  int         `json:"bits"`
	Dictionary interface{} `json:"dict"`
}

// Node represents a node in the Huffman tree
type Node struct {
	char  rune
	freq  int
	left  *Node
	right *Node
}

// PriorityQueue implements heap.Interface and holds Nodes
type PriorityQueue []*Node

func (pq PriorityQueue) Len() int            { return len(pq) }
func (pq PriorityQueue) Less(i, j int) bool  { return pq[i].freq < pq[j].freq }
func (pq PriorityQueue) Swap(i, j int)       { pq[i], pq[j] = pq[j], pq[i] }
func (pq *PriorityQueue) Push(x interface{}) { *pq = append(*pq, x.(*Node)) }
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

// BuildHuffmanTree builds the Huffman tree and returns the root
func BuildHuffmanTree(freqMap map[rune]int) *Node {
	pq := make(PriorityQueue, 0)
	heap.Init(&pq)

	for char, freq := range freqMap {
		heap.Push(&pq, &Node{char: char, freq: freq})
	}

	for pq.Len() > 1 {
		left := heap.Pop(&pq).(*Node)
		right := heap.Pop(&pq).(*Node)
		heap.Push(&pq, &Node{freq: left.freq + right.freq, left: left, right: right})
	}

	return heap.Pop(&pq).(*Node)
}

// GenerateHuffmanCodes generates the Huffman codes for each character
func GenerateHuffmanCodes(node *Node, prefix string, huffmanCodes map[rune]string) {
	if node == nil {
		return
	}
	if node.left == nil && node.right == nil {
		huffmanCodes[node.char] = prefix
	}
	GenerateHuffmanCodes(node.left, prefix+"0", huffmanCodes)
	GenerateHuffmanCodes(node.right, prefix+"1", huffmanCodes)
}

// EncodeString encodes the input string to a bit string using Huffman encoding
func EncodeString(input string, huffmanCodes map[rune]string) string {
	var bitString strings.Builder
	for _, char := range input {
		bitString.WriteString(huffmanCodes[char])
	}
	return bitString.String()
}

// FrequencyMap creates a frequency map for the input string
func FrequencyMap(input string) map[rune]int {
	freqMap := make(map[rune]int)
	for _, char := range input {
		freqMap[char]++
	}
	return freqMap
}

func BitStringToBytes(bitString string) ([]byte, int, error) {
	var bytes []byte
	length := len(bitString)

	// Pad the bitString with trailing zeros if its length is not a multiple of 8
	if length%8 != 0 {
		padding := 8 - (length % 8)
		bitString += fmt.Sprintf("%0*s", padding, "0")
	}

	for i := 0; i < len(bitString); i += 8 {
		byteStr := bitString[i : i+8]
		b, err := strconv.ParseUint(byteStr, 2, 8)
		if err != nil {
			return nil, 0, err
		}
		bytes = append(bytes, byte(b))
	}

	return bytes, length, nil
}

// encodeBitString adds the actual bit length as metadata and converts to a byte slice
func EncodeBitString(bitString string) ([]byte, error) {
	bytes, bitLength, err := BitStringToBytes(bitString)
	if err != nil {
		return nil, err
	}

	// Create a buffer and write the actual bit length as the first 4 bytes
	buf := new(byteslib.Buffer)

	err = binary.Write(buf, binary.LittleEndian, int32(bitLength))
	if err != nil {
		return nil, err
	}

	// Write the actual encoded bytes
	buf.Write(bytes)
	return buf.Bytes(), nil
}

type Attribute struct {
	Index    int    `json:"-"`
	Name     string `json:"n"`
	OptCount int    `json:"-"`
	Marked   bool   `json:"-"`
}

type AttributeOccurrence struct {
	Name  string `json:"n"`
	Times int    `json:"-"`
	MapTo string `json:"m"`
}

var attr = make([]Attribute, 0)
var attrOptCnt = make(map[string]map[string]AttributeOccurrence)
var outputMap = make(map[string][]AttributeOccurrence)

var wg sync.WaitGroup

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

func CompressRecordsTrivial(records [][]string) [][]string {
	ret := make([][]string, 0)
	for _, record := range records {
		for i := 0; i < len(attr); i++ {
			if attr[i].Marked {
				record[attr[i].Index] = attrOptCnt[attr[i].Name][record[attr[i].Index]].MapTo
			}
		}
		ret = append(ret, record)
	}
	return ret
}

func CompressRecords(records [][]string, end int) map[string]interface{} {
	var compressor = make(map[string]interface{})
	for _, record := range records {
		var iter = compressor
		for i := 0; i < end; i++ {
			if attr[i].Marked {
				// Map key to value
				record[attr[i].Index] = attrOptCnt[attr[i].Name][record[attr[i].Index]].MapTo
			}
			if iter[record[attr[i].Index]] == nil {
				if i < end-1 {
					iter[record[attr[i].Index]] = make(map[string]interface{})
				} else if i == end-1 {
					iter[record[attr[i].Index]] = make([]interface{}, 0)
				}
			}
			// iter = (map[string]interface{})(iter[record[attr[i].index]])
			if i != end-1 {
				value, _ := iter[record[attr[i].Index]].(map[string]interface{})
				iter = value
			} else {
				temp := make([]interface{}, 0)
				for j := i + 1; j < len(attr); j++ {
					temp = append(temp, record[attr[j].Index])
				}
				// iter[record[attr[i].Index]] = temp
				if iter[record[attr[i].Index]] == nil {
					iter[record[attr[i].Index]] = make([]interface{}, 0)
				}
				iter[record[attr[i].Index]] = append(iter[record[attr[i].Index]].([]interface{}), temp)
			}
		}
	}
	return compressor
}

func ResetOrder(attrs []Attribute, orders []string) ([]Attribute, error) {
	ret := make([]Attribute, 0)
	for _, order := range orders {
		var found Attribute
		flag := false
		for _, attr := range attrs {
			if attr.Name == order {
				// fmt.Println(order, attr.name)
				found = attr
				flag = true
			}
		}
		if !flag {
			return nil, errors.New("Didn't find correspond attribute name:" + order)
		}
		ret = append(ret, found)
	}
	return ret, nil
}

var memProfileFile *os.File

func startMemoryProfiling(filename string) {
	var err error
	memProfileFile, err = os.Create(filename)
	if err != nil {
		panic("could not create memory profile")
	}
	defer memProfileFile.Close()
	if err = pprof.WriteHeapProfile(memProfileFile); err != nil {
		panic("dump memory failed !")
	}

	log.Println("Memory Profiling End!")
}

var path *string
var chunk *int
var dirname *string
var cores *int
var enableHuffman *bool
var enableTrie *bool
var isDecompress *bool
var notAlibaba *bool

func main() {

	path = flag.String("path", "", "file to be compressed")
	chunk = flag.Int("chunk", 0, "chunk size")
	dirname = flag.String("dirname", "output", "output directory name")
	cores = flag.Int("j", 1, "set max cpu core using")
	enableHuffman = flag.Bool("huffman", false, "use huffman encoding")
	enableTrie = flag.Bool("merging", false, "use Merging Tree compression")
	isDecompress = flag.Bool("decompress", false, "whether is decompressing files.")
	notAlibaba = flag.Bool("not_alibaba", false, "sort attributes by optional value count if true, otherwise use predefined order")

	flag.Parse()
	if !*isDecompress {
		compress()
	} else {
		reconstruct(*dirname)
	}
}

func compress() {

	if *path == "" {
		fmt.Println("Path parameter is required")
		return
	}
	file, err := os.Open(*path)
	if err != nil {
		fmt.Print(err)
		return
	}
	runtime.GOMAXPROCS(*cores)
	defer file.Close()

	reader := csv.NewReader(file)
	count := 0

	var attrExists = make([]map[string]bool, 0)
	var lenAttr int
	var records = make([][]string, 0)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			// some records has error offset
			// for example, in this dataset there is 11 attribute,
			// but some records got 12 values.
			continue
		}
		count++
		if count == 1 {
			for record[len(record)-1] == "" {
				record = record[0 : len(record)-1]
			}
			fmt.Println(record)
			lenAttr = len(record)
			for i := 0; i < lenAttr; i++ {
				attr = append(attr, Attribute{
					Index:    i,
					Name:     record[i],
					OptCount: 0,
					Marked:   false,
				})
				attrExists = append(attrExists, make(map[string]bool))
				attrOptCnt[record[i]] = make(map[string]AttributeOccurrence)
			}
		} else {

			for record[len(record)-1] == "" {
				record = record[0 : len(record)-1]
			}
			lenRecord := len(record)
			records = append(records, record)
			if lenRecord != lenAttr {
				fmt.Println(record)
				panic("length not coresspond!")
			}
			for i := 0; i < lenRecord; i++ {
				if attr[i].Name == "dminstanceid" || attr[i].Name == "uminstanceid" {
					// Most of instance id is written in pattern MS_{UM/DM}_POD_{instance_id}
					// we delete its prefix.
					strs := strings.Split(record[i], "_")
					if len(strs) == 4 {
						record[i] = strs[3]
					}
				} else {
					strs := strings.Split(record[i], "_")
					// Value of `trace_id` is written in pattern T_{number},
					// so does `serivce`, `um`, `dm`. we delete the prefix.
					if len(strs) > 1 {
						record[i] = strs[1]
					}
				}
				if !attrExists[i][record[i]] {
					attr[i].OptCount++
					attrOptCnt[attr[i].Name][record[i]] = AttributeOccurrence{
						Name:  record[i],
						Times: 1,
					}
				} else {
					attrOptCnt[attr[i].Name][record[i]] = AttributeOccurrence{
						Name:  record[i],
						Times: attrOptCnt[attr[i].Name][record[i]].Times + 1,
					}
				}
				attrExists[i][record[i]] = true
			}
		}
	}
	// fmt.Println(attrOptCnt)
	for i, item := range attr {
		if item.OptCount > count/100 {
			continue
		}
		fmt.Println(item.Name, item.OptCount)
		attr[i].Marked = true
		temp := make([]AttributeOccurrence, 0)
		for _, value := range attrOptCnt[item.Name] {
			temp = append(temp, value)
		}
		sort.Slice(temp, func(i, j int) bool {
			return temp[i].Times > temp[j].Times
		})
		for index, iter := range temp {
			attrOptCnt[item.Name][iter.Name] = AttributeOccurrence{
				Times: attrOptCnt[item.Name][iter.Name].Times,
				Name:  attrOptCnt[item.Name][iter.Name].Name,
				MapTo: Number2String(index),
			}
			temp[index].MapTo = Number2String(index)
		}
		outputMap[item.Name] = temp
	}
	var orders = []string{"rpctype", "service", "um", "dm", "interface", "traceid", "uminstanceid", "dminstanceid", "rpc_id", "rt", "timestamp"}
	if *notAlibaba {
		// 按照 optional value 值从小到大排序
		sort.Slice(attr, func(i, j int) bool {
			return attr[i].OptCount < attr[j].OptCount
		})
		fmt.Println("Attributes sorted by optional value count")
	} else {
		// 按照指定顺序排序
		attr, err = ResetOrder(attr, orders)
		if err != nil {
			fmt.Printf("Error Occurs When Reset Attributes Order: %s", err.Error())
			return
		}
		fmt.Println("Attributes sorted by predefined order")
	}
	if *chunk == 0 {
		*chunk = count
	}
	fmt.Println("[Start Compression] Total Records:", count, "Chunk Size:", *chunk)
	err = os.Mkdir("./"+*dirname, 0755)
	if err != nil {
		fmt.Println("Error occurred while creating folder:", err)
		return
	}
	var total time.Duration = 0

	for i := 0; i < count; i += *chunk {
		wg.Add(1)
		start := i
		end := (func(a int, b int) int {
			if a < b {
				return a
			} else {
				return b
			}
		})(i+*chunk-1, count-1)
		recordsSlice := records[start:end]
		func() {
			fileName := fmt.Sprintf("./%s/chunk_%d_%d.trie", *dirname, start, end)

			file, err := os.Create(fileName)
			if err != nil {
				fmt.Println("Create File", fileName, "Failed! Reason:", err.Error())
			}
			start_ := time.Now()
			defer file.Close()
			var contents []byte

			// flag: false: trie + dict; true: dict only
			if !*enableTrie {
				// contents, err = json.Marshal(CompressRecordsTrivial(recordsSlice))
				err = nil
				str := new(byteslib.Buffer)
				res := CompressRecordsTrivial(recordsSlice)
				for _, record := range res {
					for _, item := range record {
						str.Write([]byte(item))
						str.Write([]byte(" "))
					}
					str.Write([]byte("\n"))
				}
				contents = str.Bytes()
			} else {
				contents, err = json.Marshal(CompressRecords(recordsSlice, len(attr)-4))
			}

			if err != nil {
				fmt.Println("Marshal Records", fileName, "Failed! Reason:", err.Error())
			}
			if *enableHuffman {
				huffmanTreeName := fmt.Sprintf("./%s/chunk_%d_%d_huffman.json", *dirname, start, end)
				huffmanFile, err := os.Create(huffmanTreeName)
				if err != nil {
					fmt.Println("Create File", huffmanTreeName, "Failed! Reason:", err.Error())
				}
				beforeHuffman := string(contents)
				freqMap := FrequencyMap(beforeHuffman)
				huffmanTreeRoot := BuildHuffmanTree(freqMap)
				huffmanCodes := make(map[rune]string)
				GenerateHuffmanCodes(huffmanTreeRoot, "", huffmanCodes)
				bitString := EncodeString(beforeHuffman, huffmanCodes)
				encodedBytes, err := EncodeBitString(bitString)
				if err != nil {
					fmt.Println("Error encoding:", err)
					return
				}

				contentsHuffman, err := json.Marshal(HuffmanDictionary{
					BitNumber:  len(bitString),
					Dictionary: huffmanCodes,
				})
				if err != nil {
					fmt.Println("Error Marshaling Huffman Codes:", err)
					return
				}
				huffmanFile.Write(contentsHuffman)
				_, err = file.Write(encodedBytes)
				if err != nil {
					fmt.Println("Write files", fileName, "Failed! Reason:", err.Error())
				}
			} else {
				elapsed := time.Since(start_)
				fmt.Println(start_)
				fmt.Println(time.Now())
				total += elapsed
				_, err = file.Write(contents)
				if err != nil {
					fmt.Println("Write files", fileName, "Failed! Reason:", err.Error())
				}
			}
			if i == 0 {
				startMemoryProfiling(*dirname + "_mem.prof")
			}
			fmt.Printf("Task %s Complete \n", fileName)
			runtime.GC()
			wg.Done()
		}()
	}
	wg.Wait()

	fmt.Printf("Time of Execution: %s\n", total)
	fileName := fmt.Sprintf("./%s/attributes_order.json", *dirname)
	file, err = os.Create(fileName)
	if err != nil {
		fmt.Println("Create Files", fileName, "Failed! Reason:", err.Error())
		return
	}
	contents, err := json.Marshal(attr)
	if err != nil {
		fmt.Println("Marshal Records", "attributes_order.json", "Failed! Reason:", err.Error())
		return
	}
	_, err = file.Write(contents)
	if err != nil {
		fmt.Println("Write files", "attributes_order.json", "Failed! Reason:", err.Error())
		return
	}
	fileName = fmt.Sprintf("./%s/dictionary.json", *dirname)
	file, err = os.Create(fileName)
	if err != nil {
		fmt.Println("Create Files", fileName, "Failed! Reason:", err.Error())
		return
	}
	contents, err = json.Marshal(outputMap)
	if err != nil {
		fmt.Println("Marshal Records", "dictionary.json", "Failed! Reason:", err.Error())
		return
	}
	_, err = file.Write(contents)
	if err != nil {
		fmt.Println("Write files", "dictionary.json", "Failed! Reason:", err.Error())
		return
	}
	fmt.Printf("Task %s Complete \n", fileName)
	fmt.Println("All Task Complete.")
	printMemUsage()
}
