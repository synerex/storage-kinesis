package main

// Fluentd-protocol / storage / AMP / Kinesis Provider

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/golang/protobuf/proto"
	fluentd "github.com/synerex/proto_fluentd"
	pb "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"
	sxutil "github.com/synerex/synerex_sxutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	idlist          []uint64
	spMap           map[uint64]*sxutil.SupplyOpts
	mu              sync.Mutex
	sxServerAddress string
	svc				*kinesis.Kinesis
	lastLogTime	time.Time
	lastLogCount    = 0
)

type Channel struct {
	Channel string `json:"channel"`
}


func init() {
    idlist = make([]uint64, 0)
	spMap = make(map[uint64]*sxutil.SupplyOpts)
	initKinesis()
}


func jsonDecode(jsonByte []byte) map[string]interface{} {
	var dt map[string]interface{}
	err := json.Unmarshal(jsonByte, &dt)
	if err == nil {
		return dt
	}
	fmt.Println("jsonDecodeErr:", err)
	return nil
}

func base64UnCompress(str string) []byte {
	data, _ := base64.StdEncoding.DecodeString(str)
	dt1, err := zlib.NewReader(bytes.NewReader(data))
	if err == nil {
		buf, err := ioutil.ReadAll(dt1)
		if err == nil {
			return buf
		} else {
			fmt.Println("base64UncompErr:", err)
		}
	} else {
		fmt.Println("base64UncompErr:", err)
	}
	return []byte(" ")
}

var totalTerminals int32
var terminals map[string]*Terminal
var amps map[string]*AMPM

type Terminal struct {
	lastTS     float64
	lastAMP    string
	AMPS       []string // slice of seen AMPs
	timestamps []string // slice of seen AMPs
	powers     []string // slice of seen AMPs
	//	oids       []string // slice of seen AMPs
	counts  []string // slice of seen AMPs
	termstr string   // infoString
	count   int32    // howmany count
}

type AMPM struct { // for signal info
	AMPname string
	lastTS  int64
	count   int32
}


func initKinesis(){
	lastLogTime = time.Now()
	sess := session.Must(session.NewSession())
	svc = kinesis.New(sess, aws.NewConfig().WithRegion("ap-northeast-1"))
}

func sendDataToKinesis(data []byte){
	record := &kinesis.PutRecordInput{
		Data:	data,
		PartitionKey: aws.String("sxaws"),
		StreamName:   aws.String("AMPstream"),
	}

    var count time.Duration = 1
	for {
        res , err :=	svc.PutRecord(record)
        if err == nil {

            // if log speed is too high
            cur := time.Now()
            if cur.Sub(lastLogTime) > time.Second*2 {
                log.Printf("KinesisRes: skip %d:seq: %s:%s\n",lastLogCount, *res.SequenceNumber,*res.ShardId )
                lastLogTime = cur
                lastLogCount = 0
            }else {lastLogCount++}
            break
        }
        log.Printf("Error on putRecord to Kinesis: %v",err)
        // we need to retry
        
        count *=2
        if count > 50000 {
            // Kinesis may crash?
            log.Printf("Retry count is over 50000 <  %d ,  now send failed ",count)
            return
        }
        time.Sleep(count * time.Millisecond )
    }

}

func convertWiFi(dt map[string]interface{}) {
	host := dt["h"].(string)
	wifi := dt["d"].(string)

	wbases := strings.Split(wifi, "\n")
	for _, s := range wbases {
		vals := strings.Split(s, ",")
		//		log.Printf("Split into %d", len(vals))
		if len(vals) < 5 {
			continue
		}
		f, _ := strconv.ParseFloat(vals[0], 64)
		term := terminals[vals[1]] // get mac_id
		if term == nil {           // add new terminal
			terminals[vals[1]] = &Terminal{
				lastTS:     f,
				lastAMP:    host,
				AMPS:       make([]string, 0),
				timestamps: make([]string, 0),
				powers:     make([]string, 0),
				//				oids:       make([]string, 0),
				counts:  make([]string, 0),
				termstr: s,
				count:   0,
			}
			term = terminals[vals[1]]
		}

		term.lastAMP = host[9:]
		term.lastTS = f
		term.AMPS = append(term.AMPS, host[9:])
		term.timestamps = append(term.timestamps, vals[0])
		term.powers = append(term.powers, vals[2])
		//		term.oids = append(term.oids, vals[3])
		term.counts = append(term.counts, vals[4])
		term.count = term.count + 1
		totalTerminals++
//		log.Printf("%v", *term)

//		sendData := vals[0]+","+host[9:]+","+vals[1]+","+vals[2]+","+vals[3]+","+vals[4]
		sendData := "{\"ts\":"+vals[0]+",\"amp\":\""+host[9:]+"\",\"hmac\":\""+vals[1]+"\",\"rssi\":"+vals[2]+",\"oui\":\""+vals[3][:6]+"\",\"type\":"+vals[3][7:]+",\"ct\":"+vals[4] +"}"

		sendDataToKinesis([]byte(sendData))
	
	}

	log.Printf("WiFi from %s wifi [%d] terminal count %d/%d\n", host, len(wbases)-1, len(terminals), totalTerminals)
}

func checkAMPM() string {
	tm := time.Now().Unix()
	st := make([]string, 0)

	for n, v := range amps {
		nm := n[10:] // slice from "AMPM18-HZ0XX"
		if tm-v.lastTS < 10 {
			st = append(st, nm)
		}
	}
	sort.Slice(st, func(i, j int) bool {
		ii, _ := strconv.Atoi(st[i])
		jj, _ := strconv.Atoi(st[j])
		return ii < jj
	})

	return fmt.Sprintf("%d/%d %v", len(st), len(amps), st)

}

// callback for each Supply
func supplyCallback(clt *sxutil.SXServiceClient, sp *pb.Supply) {
	// check if demand is match with my supply.
	//	log.Println("Got Fluentd Supply callback")

	record := &fluentd.FluentdRecord{}
	err := proto.Unmarshal(sp.Cdata.Entity, record)

	if err == nil {
		//		log.Println("Got record:", record.Tag, record.Time)
		recordStr := *(*string)(unsafe.Pointer(&(record.Record)))
		replaced := strings.Replace(recordStr, "=>", ":", 1)
		dt0 := jsonDecode([]byte(replaced))
		if dt0 != nil {
			buf := base64UnCompress(dt0["m"].(string))
			if len(buf) > 1 {
				dt := jsonDecode(buf)
				if dt != nil {
					if record.Tag == "ampsense.pack.test.signal" {
						//				log.Printf("ID:%v, %v, %v", dt["a"], dt["ts"], dt["g"])
						ampName := dt["a"].(string)
						amp := amps[ampName]
						if amp == nil {
							amps[ampName] = &AMPM{
								AMPname: ampName,
								count:   0,
							}
							amp = amps[ampName]
						}
						amp.lastTS = time.Now().Unix()
						amp.count++
						sxutil.SetNodeStatus(int32(len(terminals)), checkAMPM())
					} else if record.Tag == "ampsense.pack.packet.test" {
						//						log.Printf("packet:%v\n", dt)
						convertWiFi(dt)
					} else { // unknown data.
						log.Printf("UNmarshal Result: %s, %v\n", record.Tag, dt)
					}
				}
			}
		}
	}

}

func subscribeSupply(client *sxutil.SXServiceClient) {
	// goroutine!
	ctx := context.Background() //
	client.SubscribeSupply(ctx, supplyCallback)
	// comes here if channel closed
	log.Printf("Server closed... on Kinesis provider")
}

func main() {
	terminals = make(map[string]*Terminal)
	amps = make(map[string]*AMPM)
	totalTerminals = 0

	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)

	channelTypes := []uint32{pbase.FLUENTD_SERVICE}
	// obtain synerex server address from nodeserv
	srv, err := sxutil.RegisterNode(*nodesrv, "Storege-Kinesis", channelTypes, nil)
	if err != nil {
		log.Fatal("Can't register node...")
	}
	log.Printf("Connecting Server [%s]\n", srv)

	wg := sync.WaitGroup{} // for syncing other goroutines
	sxServerAddress = srv
	client := sxutil.GrpcConnectServer(srv)
	argJson := fmt.Sprintf("{Client:Kinesis}")
	sclient := sxutil.NewSXServiceClient(client, pbase.FLUENTD_SERVICE, argJson)

	wg.Add(1)
	subscribeSupply(sclient)

	wg.Wait()
	sxutil.CallDeferFunctions() // cleanup!

}


