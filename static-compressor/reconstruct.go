package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func reconstruct(dirPath string) {
	dirPath = strings.TrimPrefix(dirPath, "/")

	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		panic(err)
	}

	notFile := map[string]bool{
		"dictionary.json":       true,
		"attributes_order.json": true,
	}

	filteredFiles := []string{}
	for _, file := range files {
		if !file.IsDir() && !notFile[file.Name()] {
			filteredFiles = append(filteredFiles, file.Name())
		}
	}

	dictionaryPath := filepath.Join(dirPath, "dictionary.json")
	attributesOrderPath := filepath.Join(dirPath, "attributes_order.json")

	dictionaryData, err := ioutil.ReadFile(dictionaryPath)
	if err != nil {
		panic(err)
	}
	attributesOrderData, err := ioutil.ReadFile(attributesOrderPath)
	if err != nil {
		panic(err)
	}

	var dictionary map[string][]map[string]string
	if err := json.Unmarshal(dictionaryData, &dictionary); err != nil {
		panic(err)
	}

	dict_ := make(map[string]map[string]string)
	for key, items := range dictionary {
		dict_[key] = make(map[string]string)
		for _, item := range items {
			dict_[key][item["m"]] = item["n"]
		}
	}

	var attributesOrder []map[string]string
	if err := json.Unmarshal(attributesOrderData, &attributesOrder); err != nil {
		panic(err)
	}

	translate := func(item string, depth int) string {
		if val, ok := dict_[attributesOrder[depth]["n"]]; ok && len(val) > 0 {
			if translated, ok := val[item]; ok {
				return translated
			}
		}
		return item
	}

	var retrieve func(trie interface{}, depth int) []map[string]string
	retrieve = func(trie interface{}, depth int) []map[string]string {
		if trieSlice, ok := trie.([]interface{}); ok {
			var results []map[string]string
			for _, items := range trieSlice {
				result := make(map[string]string)
				for index, item := range items.([]interface{}) {
					result[attributesOrder[depth+index]["n"]] = item.(string)
				}
				results = append(results, result)
			}
			return results
		}

		trieMap, ok := trie.(map[string]interface{})
		if !ok {
			fmt.Println(trie)
			fmt.Println(depth)
			panic("convert type failed")
		}
		keys := make([]string, 0, len(trieMap))
		for key := range trieMap {
			keys = append(keys, key)
		}

		var ret []map[string]string
		for _, key := range keys {
			o := retrieve(trieMap[key], depth+1)
			for _, item := range o {
				item[attributesOrder[depth]["n"]] = translate(key, depth)
			}
			ret = append(ret, o...)
		}
		return ret
	}

	for index, file := range filteredFiles {
		filePath := filepath.Join(dirPath, file)
		trieData, err := ioutil.ReadFile(filePath)
		if err != nil {
			panic(err)
		}

		var trie interface{}
		if err := json.Unmarshal(trieData, &trie); err != nil {
			panic(err)
		}

		start := time.Now()
		data := retrieve(trie, 0)
		duration := time.Since(start)

		for _, item := range data {
			if dm, ok := item["dm"]; ok && dm != "USER" && dm != "UNKNOWN" && dm != "UNAVALIBLE" {
				item["dm"] = "MS_" + dm
				if item["dminstanceid"] != "UNKNOWN" {
					item["dminstanceid"] = item["dm"] + "_POD_" + item["dminstanceid"]
				}
			}
			if um, ok := item["um"]; ok && um != "USER" && um != "UNKNOWN" && um != "UNAVALIBLE" {
				item["um"] = "MS_" + um
				if item["uminstanceid"] != "UNKNOWN" {
					item["uminstanceid"] = item["um"] + "_POD_" + item["uminstanceid"]
				}
			}
			if _, ok := item["traceid"]; ok {
				item["traceid"] = "T_" + item["traceid"]
			}
			if _, ok := item["service"]; ok {
				item["service"] = "S_" + item["service"]
			}
		}

		csvFile, err := os.Create(fmt.Sprintf("result_%d.csv", index))
		if err != nil {
			panic("error creating CSV file: " + err.Error())
		}
		defer csvFile.Close()

		writer := csv.NewWriter(csvFile)
		defer writer.Flush()

		if len(data) > 0 {
			// Write CSV header
			headers := make([]string, 0, len(data[0]))
			for key := range data[0] {
				headers = append(headers, key)
			}
			if err := writer.Write(headers); err != nil {
				panic("error writing CSV header: " + err.Error())
			}

			// Write CSV rows
			for _, item := range data {
				row := make([]string, len(headers))
				for i, header := range headers {
					row[i] = item[header]
				}
				if err := writer.Write(row); err != nil {
					panic("error writing CSV row: " + err.Error())
				}
			}
		}

		fmt.Printf("File %s processed in %v\n", file, duration)
	}
}
