package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"os"
	"strconv"

	"gopkg.in/mgo.v2/bson"

	"github.com/gomodule/redigo/redis"

	"github.com/edgexfoundry/edgex-go/internal/pkg/db"
	"github.com/edgexfoundry/edgex-go/pkg/models"
)

type handler func(map[string]interface{})

func processEvent(jsonMap map[string]interface{}) {
	var err error

	// See go/src/github.com/edgexfoundry/edgex-go/internal/pkg/db/redis/event.go
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

	// See go/src/github.com/edgexfoundry/edgex-go/internal/pkg/db/redis/data.go:addEvent
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

	setId := jsonMap["_id"].(map[string]interface{})["$oid"].(string)
	r := models.Reading{
		Id:       bson.ObjectIdHex(setId),
		Pushed:   int64(jsonMap["pushed"].(float64)),
		Created:  int64(jsonMap["created"].(float64)),
		Origin:   int64(jsonMap["origin"].(float64)),
		Modified: int64(jsonMap["modified"].(float64)),
		Name:     jsonMap["name"].(string),
		Value:    jsonMap["value"].(string),
	}

	redisConn.Send("MULTI")
	marshalled, _ := bson.Marshal(r)
	redisConn.Send("SET", setId, marshalled)
	redisConn.Send("ZADD", db.ReadingsCollection, 0, setId)
	redisConn.Send("ZADD", db.ReadingsCollection+":created", r.Created, setId)
	redisConn.Send("ZADD", db.ReadingsCollection+":device:"+r.Device, r.Created, setId)
	redisConn.Send("ZADD", db.ReadingsCollection+":name:"+r.Name, r.Created, setId)
	_, err = redisConn.Do("EXEC")
	if err != nil {
		log.Fatal(err)
	}
}

func processValueDescriptors(jsonMap map[string]interface{}) {
	var err error

	setId := jsonMap["_id"].(map[string]interface{})["$oid"].(string)
	valueDesc := models.ValueDescriptor{
		Id:           bson.ObjectIdHex(setId),
		Created:      int64(jsonMap["created"].(float64)),
		Modified:     int64(jsonMap["modified"].(float64)),
		Origin:       int64(jsonMap["origin"].(float64)),
		Name:         jsonMap["name"].(string),
		Min:          jsonMap["min"].(string),
		Max:          jsonMap["max"].(string),
		DefaultValue: jsonMap["defaultValue"].(string),
		Type:         jsonMap["type"].(string),
		UomLabel:     jsonMap["uomLabel"].(string),
		Formatting:   jsonMap["formatting"].(string),
	}

	labelInterfaces := jsonMap["labels"].([]interface{})
	valueDesc.Labels = make([]string, len(labelInterfaces))
	for i, v := range labelInterfaces {
		valueDesc.Labels[i] = v.(string)
	}

	redisConn.Send("MULTI")
	marshalled, _ := bson.Marshal(valueDesc)
	redisConn.Send("SET", setId, marshalled)
	redisConn.Send("ZADD", db.ValueDescriptorCollection, 0, setId)
	redisConn.Send("HSET", db.ValueDescriptorCollection+":name", valueDesc.Name, setId)
	redisConn.Send("ZADD", db.ValueDescriptorCollection+":uomlabel:"+valueDesc.UomLabel, 0, setId)
	redisConn.Send("ZADD", db.ValueDescriptorCollection+":type:"+valueDesc.Type, 0, setId)
	for _, label := range valueDesc.Labels {
		redisConn.Send("ZADD", db.ValueDescriptorCollection+":label:"+label, 0, setId)
	}

	_, err = redisConn.Do("EXEC")
	if err != nil {
		log.Fatal(err)
	}
}

func processAddressable(jsonMap map[string]interface{}) {
	var err error

	setId := jsonMap["_id"].(map[string]interface{})["$oid"].(string)
	a := models.Addressable{
		BaseObject: models.BaseObject{
			Created:  int64(jsonMap["created"].(float64)),
			Modified: int64(jsonMap["modified"].(float64)),
			Origin:   int64(jsonMap["origin"].(float64)),
		},
		Id:         bson.ObjectIdHex(setId),
		Name:       jsonMap["name"].(string),
		Protocol:   jsonMap["protocol"].(string),
		HTTPMethod: jsonMap["method"].(string),
		Address:    jsonMap["address"].(string),
		Port:       jsonMap["port"].(int),
		Path:       jsonMap["path"].(string),
		Publisher:  jsonMap["publisher"].(string),
		User:       jsonMap["user"].(string),
		Password:   jsonMap["password"].(string),
		Topic:      jsonMap["topic"].(string),
	}

	redisConn.Send("MULTI")
	marshalled, _ := bson.Marshal(a)
	redisConn.Send("SET", setId, marshalled)
	redisConn.Send("ZADD", db.Addressable, 0, setId)
	redisConn.Send("SADD", db.Addressable+":topic:"+a.Topic, setId)
	redisConn.Send("SADD", db.Addressable+":port:"+strconv.Itoa(a.Port), setId)
	redisConn.Send("SADD", db.Addressable+":publisher:"+a.Publisher, setId)
	redisConn.Send("SADD", db.Addressable+":address:"+a.Address, setId)
	redisConn.Send("HSET", db.Addressable+":name", a.Name, setId)
	_, err = redisConn.Do("EXEC")
	if err != nil {
		log.Fatal(err)
	}
}

func readOptionalString(i interface{}) string {
	if i == nil {
		return ""
	} else {
		return i.(string)
	}
}

func processCommand(jsonMap map[string]interface{}) {
	var err error

	setId := jsonMap["_id"].(map[string]interface{})["$oid"].(string)
	c := models.Command{
		BaseObject: models.BaseObject{
			Created:  int64(jsonMap["created"].(float64)),
			Modified: int64(jsonMap["modified"].(float64)),
			Origin:   int64(jsonMap["origin"].(float64)),
		},
		Id:   bson.ObjectIdHex(setId),
		Name: readOptionalString(jsonMap["name"]),
		Get: &models.Get{
			Action: models.Action{
				Path:      readOptionalString(jsonMap["get"].(map[string]interface{})["path"]),
				URL:       readOptionalString(jsonMap["get"].(map[string]interface{})["url"]),
				Responses: nil, // XXX sample data should be array
			},
		},
		Put: &models.Put{
			Action: models.Action{
				Path:      readOptionalString(jsonMap["get"].(map[string]interface{})["path"]),
				URL:       readOptionalString(jsonMap["get"].(map[string]interface{})["url"]),
				Responses: nil, // XXX sample data should be array
			},
			ParameterNames: nil, // XXX inconsistent with sample data
		},
	}

	redisConn.Send("MULTI")
	marshalled, _ := bson.Marshal(c)
	redisConn.Send("SET", setId, marshalled)
	redisConn.Send("ZADD", db.Command, 0, setId)
	redisConn.Send("SADD", db.Command+":name:"+c.Name, setId)
	_, err = redisConn.Do("EXEC")
	if err != nil {
		log.Fatal(err)
	}
}

func processDevice(jsonMap map[string]interface{}) {
	var err error

	setId := jsonMap["_id"].(map[string]interface{})["$oid"].(string)
	d := models.Device{
		DescribedObject: models.DescribedObject{
			BaseObject: models.BaseObject{
				Created:  int64(jsonMap["created"].(float64)),
				Modified: int64(jsonMap["modified"].(float64)),
				Origin:   int64(jsonMap["origin"].(float64)),
			},
			Description: jsonMap["description"].(string),
		},
		Id:             bson.ObjectIdHex(setId),
		Name:           jsonMap["name"].(string),
		AdminState:     jsonMap["adminState"].(models.AdminState),
		OperatingState: jsonMap["operatingState"].(models.OperatingState),
		Addressable:    models.Addressable{}, // XXX inconsistent with sample data
		LastConnected:  int64(jsonMap["lastConnected"].(float64)),
		LastReported:   int64(jsonMap["lastReported"].(float64)),
		Service:        models.DeviceService{}, // XXX inconsistent with sample data
		Profile:        models.DeviceProfile{}, // XXX inconsistent with sample data
	}

	labelInterfaces := jsonMap["labels"].([]interface{})
	d.Labels = make([]string, len(labelInterfaces))
	for i, v := range labelInterfaces {
		d.Labels[i] = v.(string)
	}

	redisConn.Send("MULTI")
	marshalled, _ := bson.Marshal(c)
	redisConn.Send("SET", setId, marshalled)
	redisConn.Send("ZADD", db.Device, 0, setId)
	redisConn.Send("HSET", db.Device+":name", d.Name, setId)
	redisConn.Send("SADD", db.Device+":addressable:"+d.Addressable.Id.Hex(), setId)
	redisConn.Send("SADD", db.Device+":service:"+d.Service.Id.Hex(), setId)
	redisConn.Send("SADD", db.Device+":profile:"+d.Profile.Id.Hex(), setId)
	for _, label := range d.Labels {
		redisConn.Send("SADD", db.Device+":label:"+label, setId)
	}
	_, err = redisConn.Do("EXEC")
	if err != nil {
		log.Fatal(err)
	}
}

func processDeviceProfile(jsonMap map[string]interface{}) {
	var err error

	setId := jsonMap["_id"].(map[string]interface{})["$oid"].(string)
	d := models.DeviceProfile{
		DescribedObject: models.DescribedObject{
			BaseObject: models.BaseObject{
				Created:  int64(jsonMap["created"].(float64)),
				Modified: int64(jsonMap["modified"].(float64)),
				Origin:   int64(jsonMap["origin"].(float64)),
			},
			Description: jsonMap["description"].(string),
		},
		Id:           bson.ObjectIdHex(setId),
		Name:         jsonMap["name"].(string),
		Manufacturer: jsonMap["manufacturer"].(string),
		Model:        jsonMap["model"].(string),
		Objects:      nil, // XXX inconsistent with sample data
		Commands:     nil, // XXX inconsistent with sample data
	}

	labelInterfaces := jsonMap["labels"].([]interface{})
	d.Labels = make([]string, len(labelInterfaces))
	for i, v := range labelInterfaces {
		d.Labels[i] = v.(string)
	}

	redisConn.Send("MULTI")
	marshalled, _ := bson.Marshal(c)
	redisConn.Send("SET", setId, marshalled)
	redisConn.Send("ZADD", db.DeviceProfile, 0, setId)
	redisConn.Send("HSET", db.DeviceProfile+":name", dp.Name, setId)
	redisConn.Send("SADD", db.DeviceProfile+":manufacturer:"+dp.Manufacturer, setId)
	redisConn.Send("SADD", db.DeviceProfile+":model:"+dp.Model, setId)
	for _, label := range dp.Labels {
		redisConn.Send("SADD", db.DeviceProfile+":label:"+label, setId)
	}
	if len(dp.Commands) > 0 {
		cids := redis.Args{}.Add(db.DeviceProfile + ":commands:" + setId)
		for _, c := range dp.Commands {
			cid := c.Id.Hex()
			redisConn.Send("SADD", db.DeviceProfile+":command:"+cid, setId)
			cids = cids.Add(cid)
		}
		redisConn.Send("SADD", cids...)
	}
	_, err = redisConn.Do("EXEC")
	if err != nil {
		log.Fatal(err)
	}
}

// deviceReport
// deviceService
// provisionWatcher
// schedule
// scheduleEvent

var redisConn redis.Conn

func main() {
	var err error

	handlers := map[string]handler{
		"event":           processEvent,
		"reading":         processReading,
		"valueDescriptor": processValueDescriptors,
		"addressable":     processAddressable,
		"command":         processCommand,
		"device":          processDevice,
		"deviceProfile":   processDeviceProfile,
	}

	usage := "Type of input JSON; one of\n"
	for k := range handlers {
		usage += "\t" + k + "\n"
	}

	inputType := flag.String("t", "", usage+"Input file is read from STDIN")

	flag.Parse()
	if *inputType == "" {
		flag.Usage()
		os.Exit(1)
	}

	processor := handlers[*inputType]
	if processor == nil {
		flag.Usage()
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
