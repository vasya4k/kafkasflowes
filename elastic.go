package main

import (
	"context"
	"fmt"
	"net"

	"github.com/Shopify/sarama"
	flowprotob "github.com/cloudflare/goflow/v3/pb"
	"github.com/golang/protobuf/proto"
	elastic "github.com/olivere/elastic/v7"
	"github.com/oschwald/geoip2-golang"
	"github.com/sirupsen/logrus"
)

type FlowRecord struct {
	Type           int32    `json:"type"`
	TimeReceived   uint64   `json:"time_received"`
	SequenceNum    uint32   `json:"sequence_num"`
	TimeFlowStart  uint64   `json:"timestamp"`
	SamplingRate   uint64   `json:"sampling_rate"`
	SamplerAddress string   `json:"sampler_address"`
	TimeFlowEnd    uint64   `json:"time_flow_end"`
	Bytes          uint64   `json:"bytes"`
	Packets        uint64   `json:"packets"`
	SrcAddr        string   `json:"src_addr"`
	DstAddr        string   `json:"dst_addr"`
	Etype          uint32   `json:"etype"`
	Proto          uint32   `json:"protocol"`
	SrcPort        uint32   `json:"src_port"`
	DstPort        uint32   `json:"dst_port"`
	OutIf          uint32   `json:"out_if"`
	SrcMac         string   `json:"src_mac"`
	DstMac         string   `json:"dst_mac"`
	IPTTL          uint32   `json:"ipttl"`
	TCPFlags       uint32   `json:"tcp_flags"`
	IPv6FlowLabel  uint32   `json:"ipv6_flow_label"`
	SrcDevice      string   `json:"src_device"`
	DstDevice      string   `json:"dst_device"`
	SrcLocation    Location `json:"src_location"`
	DstLocation    Location `json:"dst_location"`
	SrcCity        string   `json:"src_city"`
	DstCity        string   `json:"dst_city"`
	SrcCountry     string   `json:"src_country"`
	DstCountry     string   `json:"dst_country"`
	SrcAS          uint     `json:"src_as"`
	DstAS          uint     `json:"dst_as"`
	SrcOrg         string   `json:"src_org"`
	DstOrg         string   `json:"dst_org"`
	PortName       string   `json:"port_name"`
	ProtoName      string   `json:"proto_name"`
	EtypeName      string   `json:"etype_name"`
}

type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type ESCfg struct {
	Address       string
	Port          string
	IPDBPath      string
	ASNDBPath     string
	MacTable      map[string]string
	GeoIPEnabled  bool
	IndexName     string
	PortTable     map[string]string
	ProtocolTable map[string]string
}

type ESIndexer struct {
	Client   *elastic.Client
	IPDB     *geoip2.Reader
	ASNDB    *geoip2.Reader
	Messages chan *sarama.ConsumerMessage
	Cfg      ESCfg
}

func NewIndexer(cfg ESCfg, messages chan *sarama.ConsumerMessage) (*ESIndexer, error) {
	var err error

	indexer := ESIndexer{
		Cfg:      cfg,
		Messages: messages,
	}

	url := fmt.Sprintf("http://%s:%s", cfg.Address, cfg.Port)

	logrus.WithFields(logrus.Fields{
		"topic": "elastic",
		"event": "start",
		"url":   url,
	}).Info("elastic init")

	indexer.Client, err = elastic.NewClient(elastic.SetSniff(false), elastic.SetURL(url))
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"topic":  "elastic",
			"event":  "new client err",
			"es_url": url,
		}).Error(err)
		return nil, err
	}
	if cfg.GeoIPEnabled {
		indexer.IPDB, err = geoip2.Open(cfg.IPDBPath)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"topic": "elastic",
				"event": "new client err",
				"path":  cfg.IPDBPath,
			}).Error(err)
			return nil, err
		}
		indexer.ASNDB, err = geoip2.Open(cfg.ASNDBPath)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"topic": "elastic",
				"event": "new client err",
				"path":  cfg.ASNDBPath,
			}).Error(err)
			return nil, err
		}
	}

	return &indexer, nil
}

func (e *ESIndexer) NewFlowDoc(msg *flowprotob.FlowMessage) *FlowRecord {
	srcMAC := fmt.Sprintf("%x", msg.SrcMac)
	dstMAC := fmt.Sprintf("%x", msg.DstMac)

	r := FlowRecord{
		Type:           int32(msg.Type),
		TimeReceived:   msg.TimeReceived,
		SequenceNum:    msg.SequenceNum,
		SamplingRate:   msg.SamplingRate,
		SamplerAddress: net.IP(msg.SamplerAddress).String(),
		TimeFlowStart:  msg.TimeFlowStart,
		TimeFlowEnd:    msg.TimeFlowEnd,
		Bytes:          msg.Bytes,
		Packets:        msg.Packets,
		SrcAddr:        net.IP(msg.SrcAddr).String(),
		DstAddr:        net.IP(msg.DstAddr).String(),
		Etype:          msg.Etype,
		Proto:          msg.Proto,
		SrcPort:        msg.SrcPort,
		DstPort:        msg.DstPort,
		OutIf:          msg.OutIf,
		SrcMac:         srcMAC,
		DstMac:         dstMAC,
		IPTTL:          msg.IPTTL,
		TCPFlags:       msg.TCPFlags,
		IPv6FlowLabel:  msg.IPv6FlowLabel,
		SrcDevice:      e.Cfg.MacTable[srcMAC],
		DstDevice:      e.Cfg.MacTable[dstMAC],
		PortName:       e.Cfg.PortTable[fmt.Sprint(msg.DstPort)],
		ProtoName:      e.Cfg.ProtocolTable[fmt.Sprint(msg.Proto)],
	}
	return &r
}

func (e *ESIndexer) SetLocationData(msg *flowprotob.FlowMessage, f *FlowRecord) {

	srcRecord, err := e.IPDB.City(net.IP(msg.SrcAddr))
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"topic": "geoip",
			"event": "dberr",
		}).Error(err)
		return
	}
	dstRecord, err := e.IPDB.City(net.IP(msg.DstAddr))
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"topic": "geoip",
			"event": "dberr",
		}).Error(err)
		return
	}

	f.SrcLocation = Location{
		Lat: srcRecord.Location.Latitude,
		Lon: srcRecord.Location.Longitude,
	}
	f.DstLocation = Location{
		Lat: dstRecord.Location.Latitude,
		Lon: dstRecord.Location.Longitude,
	}
	if srcRecord.Country.Names != nil {
		f.SrcCountry = srcRecord.Country.Names["en"]
		f.DstCountry = dstRecord.Country.Names["en"]
	}
	if srcRecord.City.Names != nil {
		f.SrcCity = srcRecord.City.Names["en"]
		f.DstCity = dstRecord.City.Names["en"]
	}

	srcASRecord, err := e.ASNDB.ASN(net.IP(msg.SrcAddr))
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"topic": "geoip",
			"event": "dberr",
		}).Error(err)
		return
	}
	f.SrcAS = srcASRecord.AutonomousSystemNumber
	f.SrcOrg = srcASRecord.AutonomousSystemOrganization

	dstASRecord, err := e.ASNDB.ASN(net.IP(msg.DstAddr))
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"topic": "geoip",
			"event": "dberr",
		}).Error(err)
		return
	}
	f.DstAS = dstASRecord.AutonomousSystemNumber
	f.DstOrg = dstASRecord.AutonomousSystemOrganization
}

func (e *ESIndexer) Index() {
	defer e.Client.Stop()
	ctx := context.Background()

	for kafkaMsg := range e.Messages {
		var sflowMsg flowprotob.FlowMessage

		err := proto.Unmarshal(kafkaMsg.Value, &sflowMsg)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"topic": "elastic",
				"event": "Unmarshal",
			}).Error(err)
			continue
		}

		doc := e.NewFlowDoc(&sflowMsg)

		if e.Cfg.GeoIPEnabled {
			e.SetLocationData(&sflowMsg, doc)
		}

		doc.TimeFlowStart = doc.TimeFlowStart * 1000

		_, err = e.Client.Index().
			Index(e.Cfg.IndexName).
			BodyJson(doc).
			Do(ctx)

		if err != nil {
			logrus.WithFields(logrus.Fields{
				"topic": "elastic",
				"event": "index_err",
			}).Error(err)
		}
	}
}
