package main

import (
	"fmt"
	"os"
	"path/filepath"
	"bufio"
	"strings"
	"strconv"
	"log"
	"net/http"
	"net/url"
	"time"
	"regexp"

	"github.com/influxdata/influxdb/client/v2"
)

var (
	influx_host = os.Getenv("DB_HOST")
	influx_port = os.Getenv("DB_PORT")
	influx_user = os.Getenv("DB_USER")
	influx_db = os.Getenv("DB_NAME")
	influx_pass = os.Getenv("DB_PASS")

	log_file = "torque_server.log"

	mandatory_args = []string{"v", "session", "id", "time"}
	dict = prepare_dict()
	influx, _ = influx_connect()
)

func main() {
	// preparing logfile
	ex, err := os.Executable()
        if err != nil {
          panic(err)
        }
        exPath := filepath.Dir(ex)
//        fmt.Println(exPath)

	f, err := os.OpenFile(exPath + "/" + log_file, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0600)
	if err != nil {
		log.Fatalf("error opening log_file: %v", err)
	}
	defer f.Close()
	log.SetOutput(f)
	log.Println("Starting up...")

	http.HandleFunc("/add", handle_add)
	log.Println("Torque_server ready on http://0.0.0.0:8000/add")
	log.Fatal(http.ListenAndServe(":8000", nil))
}

func read_file(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, scanner.Err()
}

func prepare_dict() map[string]string {
	abs_path, _ := filepath.Abs(os.Args[1])
	lines, err := read_file(abs_path)
	if err != nil {
		log.Fatalf("Error preparing dict: %s", err)
	}

	out := make(map[string]string)
	for _, line := range lines {
		split := strings.Split(string(line), ",")
		out[strings.ToLower(split[0])] = split[1]
		//fmt.Println(i, line, split)
	}
  fmt.Println("Dict loaded")
	return out
}

func influx_connect() (client.Client, error) {
	// Make client
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://" + influx_host + ":" + influx_port,
		Username: influx_user,
		Password: influx_pass,
	})
	if err != nil {
		log.Fatal("Error creating InfluxDB Client: ", err.Error())
	}
	defer c.Close()
  fmt.Println("Connected to InfluxDB")
	return c, err
}

func handle_add(w http.ResponseWriter, req *http.Request) {
	// Check params
	start := time.Now()
  fmt.Println("Processing /add")
  for i:= 0; i < len(mandatory_args); i++ {
		query := req.FormValue(mandatory_args[i])
		if query == "" {
			http.Error(w, "missing " + mandatory_args[i] + " URL parameter", http.StatusBadRequest)
			duration := time.Since(start)
			log.Printf("%s %s %s %s %s '%s'", req.RemoteAddr, req.Method, req.URL, "400", duration, "missing " + mandatory_args[i] + " URL parameter")
			return
		}
	}

	//fmt.Println(dict)

  fmt.Println("Preparing metadata")
	influx_measurement := "data"
	u, _ := url.Parse(fmt.Sprintf("%s",req.URL))
	m, _ := url.ParseQuery(strings.ToLower(u.RawQuery))

	for k, _ := range m {
		check_units, _ := regexp.MatchString("defaultunit|userunit", k)
		if check_units {
			influx_measurement = "units"
			break
		}

		check_profile, _ := regexp.MatchString("profilename", k)
		if check_profile {
			influx_measurement = "profile"
			break
		}
	}

  fmt.Println("Parsing payload")
	data := make(map[string]interface{})
	for k, v := range m {
		// skip non-data elements
		skip, _ := regexp.MatchString(`^(session|eml|id|v|time)?$`, k)
		if skip {
			//duration := time.Since(start)
			//log.Printf("%s %s %s %s %s '%s'", req.RemoteAddr, req.Method, req.URL, "206", duration, "skipping excluded key: '" + k + "'")
			continue
		}

		// skip inifinite values
		infinity, _ := regexp.MatchString(`Infinity`, v[0])
		if infinity {
			duration := time.Since(start)
			log.Printf("%s %s %s %s %s '%s'", req.RemoteAddr, req.Method, req.URL, "206", duration, "param: '" + k + "' has bad value: '" + v[0] + "'")
			continue
		}

		// normalize key using dict if possible
		key := k
		if tmp, ok := dict[k]; ok {
			key = tmp
		}

		// convert numbers to float
		matched, _ := regexp.MatchString(`^[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?$`, v[0])
		if matched {
			val, _ := strconv.ParseFloat(v[0], 64)
			data[key] = val
		} else {
			data[key] = v[0]
		}
		fmt.Println(k + " -> " + v[0] + " : " + dict[k])
	}

	fmt.Println(data)

	// adjust time
  fmt.Println("Adjust time")
  i, err := strconv.ParseInt(m["time"][0] + "000000", 10, 64)
	if err != nil {
		panic(err)
	}
	tm := time.Unix(0, i)

	// Create a new point batch
  fmt.Println("Create new point batch")
	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  influx_db,
		Precision: "ms",
	})

	// Create a point and add to batch
	tags := map[string]string{"Email": m["eml"][0], "Version": m["v"][0], "Session": m["session"][0], "Device": m["id"][0]}
	pt, err := client.NewPoint(influx_measurement, tags, data, tm)
	if err != nil {
		duration := time.Since(start)
		http.Error(w, "Error creating InfluxDB Point: " + err.Error(), http.StatusInternalServerError)
		log.Printf("%s %s %s %s %s '%s'", req.RemoteAddr, req.Method, req.URL, "500", duration, "Error creating InfluxDB Point: " + err.Error())
	}
  fmt.Println("Add point to batch")
	bp.AddPoint(pt)

	// Write the batch
  fmt.Println("Write point batch")
	if err := influx.Write(bp); err != nil {
		duration := time.Since(start)
		http.Error(w, "Error writing data to InfluxDB", http.StatusInternalServerError)
		log.Printf("%s %s %s %s %s '%s'", req.RemoteAddr, req.Method, req.URL, "500", duration, "Error writing data to InfluxDB: " + err.Error())
	}

	log.Println(pt)
	duration := time.Since(start)
	log.Printf("%s %s %s %s %s", req.RemoteAddr, req.Method, req.URL, "200", duration)
	fmt.Fprintln(w, "OK!")
}
