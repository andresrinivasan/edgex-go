package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"os"

	"gopkg.in/mgo.v2/bson"

	"github.com/edgexfoundry/edgex-go/internal/pkg/db"
	"github.com/gomodule/redigo/redis"
)

type handler func(map[string]interface{})

func processEvent(jsonMap map[string]interface{}) {
	var err error

	e := struct {
		ID       string
		Pushed   int64
		Device   string
		Created  int64
		Modified int64
		Origin   int64
	}{
		ID:       jsonMap["_id"].(map[string]interface{})["$oid"].(string),
		Pushed:   int64(jsonMap["pushed"].(float64)),
		Device:   jsonMap["device"].(string),
		Created:  int64(jsonMap["created"].(float64)),
		Modified: int64(jsonMap["modified"].(float64)),
		Origin:   int64(jsonMap["origin"].(float64)),
	}

	redisConn.Send("MULTI")
	marshalled, _ := bson.Marshal(e)
	redisConn.Send("SET", e.ID, marshalled)
	redisConn.Send("ZADD", db.EventsCollection, 0, e.ID)
	redisConn.Send("ZADD", db.EventsCollection+":created", e.Created, e.ID)
	redisConn.Send("ZADD", db.EventsCollection+":pushed", e.Pushed, e.ID)
	redisConn.Send("ZADD", db.EventsCollection+":device:"+e.Device, e.Created, e.ID)

	if len(jsonMap["readings"].([]interface{})) > 0 {
		readingIds := make([]interface{}, len(jsonMap["readings"].([]interface{}))*2+1)
		readingIds[0] = db.EventsCollection + ":readings:" + e.ID

		for i, v := range jsonMap["readings"].([]interface{}) {
			readingIds[i*2+1] = 0
			value := v.(map[string]interface{})["$id"].(map[string]interface{})["$oid"].(string)
			readingIds[i*2+2] = value
			redisConn.Send("ZADD", db.ReadingsCollection, 0, value)
		}

		redisConn.Send("ZADD", readingIds...)
	}

	_, err = redisConn.Do("EXEC")
	if err != nil {
		log.Fatal(err)
	}
}

func processReading(jsonMap map[string]interface{}) {
	var err error

	r := struct {
		ID       string
		Pushed   int64
		Created  int64
		Origin   int64
		Modified int64
		Device   string
		Name     string
		Value    string
	}{
		ID:       jsonMap["_id"].(map[string]interface{})["$oid"].(string),
		Pushed:   int64(jsonMap["pushed"].(float64)),
		Created:  int64(jsonMap["created"].(float64)),
		Origin:   int64(jsonMap["origin"].(float64)),
		Modified: int64(jsonMap["modified"].(float64)),
		Name:     jsonMap["name"].(string),
		Value:    jsonMap["value"].(string),
	}

	redisConn.Send("MULTI")
	marshalled, _ := bson.Marshal(r)
	redisConn.Send("SET", r.ID, marshalled)
	redisConn.Send("ZADD", db.ReadingsCollection+":created", r.Created, r.ID)
	redisConn.Send("ZADD", db.ReadingsCollection+":device:"+r.Device, r.Created, r.ID)
	redisConn.Send("ZADD", db.ReadingsCollection+":name:"+r.Name, r.Created, r.ID)
	_, err = redisConn.Do("EXEC")
	if err != nil {
		log.Fatal(err)
	}
}

var handlers = map[string]handler{
	"events":   processEvent,
	"readings": processReading,
}

var redisConn redis.Conn

func main() {
	var err error

	inputType := flag.String("t", "", "Type of input JSON; either events or readings")
	flag.Parse()
	if *inputType == "" {
		flag.Usage()
	}

	processor := handlers[*inputType]
	if processor == nil {
		log.Fatal("Unknown input type: " + *inputType)
	}

	redisConn, err = redis.DialURL("redis://localhost:6379")
	if err != nil {
		log.Fatal(err)
	}
	defer redisConn.Close()

	d := json.NewDecoder(os.Stdin)
	for {
		var jsonRecord interface{}

		err = d.Decode(&jsonRecord)
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Fatal(err)
		}

		processor(jsonRecord.(map[string]interface{}))
	}
}
