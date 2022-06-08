package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/honeycombio/dynsampler-go"
	"github.com/honeycombio/libhoney-go"
	"github.com/honeycombio/urlshaper"
)

const DefaultServerPort = "8080"
const KeySeperatorChar = "•"
const MaxLineLength = 65536 // set this to the maximum size we expect log lines to be

var sampler *dynsampler.EMASampleRate
var samplingFields []string
var urlFields []string

func main() {

	// Initialize and configure libhoney
	libhoney.UserAgentAddition = "http-honeylog/0.1"
	err := libhoney.Init(libhoney.Config{
		APIKey:  os.Getenv("HONEYCOMB_API_KEY"),
		Dataset: os.Getenv("HONEYCOMB_DATASET"),
	})
	if err != nil {
		fmt.Printf("fatal error initializing libhoney: %v\n", err)
		os.Exit(100)
	}
	libhoney.AddField("event.parser", "http-honeylog/0.1")
	defer libhoney.Close() // Flush any pending calls to Honeycomb

	// get sampling keys
	skeys := os.Getenv("HONEYCOMB_SAMPLING_FIELDS")
	if len(skeys) == 0 {
		fmt.Printf("fatal error: HONEYCOMB_SAMPLING_FIELDS environment variable is not set\n")
		os.Exit(101)
	}
	samplingFields = strings.Split(skeys, ",")

	// get URL fields to be parsed
	urlFields = strings.Split(os.Getenv("HONEYCOMB_URL_FIELDS"), ",")

	// Create and start sampler
	rate, err := strconv.Atoi(os.Getenv("HONEYCOMB_SAMPLE_RATE"))
	if err != nil {
		rate = 1
	}
	// Can also specify other options here for the EMADynamicSampler if desired
	sampler = &dynsampler.EMASampleRate{
		GoalSampleRate: rate,
	}
	err = sampler.Start()
	if err != nil {
		fmt.Printf("fatal error starting sampler: %v\n", err)
		os.Exit(102)
	}

	// Create HTTP server and primary handler
	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		serverPort = DefaultServerPort
	}
	server := &http.Server{Addr: ":" + serverPort}
	http.HandleFunc("/", readNewData)
	go func() {
		fmt.Printf("Starting server on port %s\n", serverPort)
		if err := server.ListenAndServe(); err != nil {
			fmt.Printf("error on server listen and serve: %v\n", err)
			os.Exit(103)
		}
	}()

	// set up signal capturing
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	// Waiting for SIGINT (kill -2)
	<-stop
	libhoney.Flush()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		fmt.Printf("error shutting down server: %v\n", err)
		os.Exit(104)
	}
}

func readNewData(w http.ResponseWriter, r *http.Request) {

	startTime := time.Now()

	scanner := bufio.NewScanner(r.Body)
	buf := make([]byte, MaxLineLength)
	scanner.Buffer(buf, MaxLineLength)

	total := 0
	success := 0
	for scanner.Scan() {
		total++

		rawData := scanner.Bytes()
		var data map[string]interface{}
		err := json.Unmarshal(rawData, &data)
		if err != nil {
			fmt.Printf("json parsing error %v, raw data: %s\n", err, string(rawData))
			continue
		}

		cleanData(data)

		rate, keep, key := determineSampleRate(data)

		if keep {
			ev := libhoney.NewEvent()
			ev.SampleRate = uint(rate)
			ev.AddField("event.samplekey", key)

			err = ev.Add(data)
			if err != nil {
				fmt.Printf("event add error %v, raw data: %s\n", err, string(rawData))
				continue
			}

			err = ev.SendPresampled()
			if err != nil {
				fmt.Printf("event send error %v, raw data: %s\n", err, string(rawData))
				continue
			}

			success++
		}
	}

	fmt.Printf("Sampled %d of %d input lines in %dms.\n", success, total, time.Now().Sub(startTime).Milliseconds())

	w.WriteHeader(200)
}

func cleanData(data map[string]interface{}) {

	// Use this to perform any general data cleanup

	for k, v := range data {
		// if value is a slice, convert to a string slice, and use a string representation of it
		// if the slice is a slice of objects this will not produce desired results
		if reflect.TypeOf(v).Kind() == reflect.Slice {
			sval := reflect.ValueOf(v)
			newVal := make([]string, sval.Len())
			for i := 0; i < sval.Len(); i++ {
				newVal[i] = fmt.Sprintf("%v", sval.Index(i).Interface())
			}
			data[k] = strings.Join(newVal, ",")
		}

		// if the field is a URL field, use urlshaper to break it out into its components
		shaper := &urlshaper.Parser{}
		for _, f := range urlFields {
			if k == f {
				res, err := shaper.Parse(fmt.Sprintf("%v", v))
				if err == nil {
					data[k+".path"] = res.Path
					for pk, pv := range res.PathFields {
						data[k+".pathFields."+pk] = strings.Join(pv, ",")
					}
					data[k+".pathShape"] = res.PathShape
					data[k+".query"] = res.Query
					for qk, qv := range res.QueryFields {
						data[k+".queryFields."+qk] = strings.Join(qv, ",")
					}
					data[k+".queryShape"] = res.QueryShape
					data[k+".uri"] = res.URI
				}
			}
			break
		}
	}
}

func determineSampleRate(data map[string]interface{}) (rate int, keep bool, key string) {

	// will determine the sample rate of an event based on sampling fields

	keys := make([]string, len(samplingFields))
	for i, field := range samplingFields {
		if val, ok := data[field]; ok {
			keys[i] = fmt.Sprintf("%v", val)
		}
	}
	key = strings.Join(keys, KeySeperatorChar)

	rate = sampler.GetSampleRate(key)
	// protect against something going weird in the sampler
	if rate < 1 {
		rate = 1
	}
	keep = rand.Intn(int(rate)) == 0
	return rate, keep, key
}
