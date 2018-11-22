package test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/agg-inventory-query/inventory"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/TerrexTech/uuuid"
	"github.com/joho/godotenv"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
)

func Byf(s string, args ...interface{}) {
	By(fmt.Sprintf(s, args...))
}

func TestInventory(t *testing.T) {
	log.Println("Reading environment file")
	err := godotenv.Load("../.env")
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	missingVar, err := commonutil.ValidateEnv(
		"KAFKA_BROKERS",
		"KAFKA_CONSUMER_EVENT_GROUP",

		"KAFKA_CONSUMER_EVENT_TOPIC",
		"KAFKA_CONSUMER_EVENT_QUERY_GROUP",
		"KAFKA_CONSUMER_EVENT_QUERY_TOPIC",

		"KAFKA_PRODUCER_EVENT_TOPIC",
		"KAFKA_PRODUCER_EVENT_QUERY_TOPIC",
		"KAFKA_PRODUCER_RESPONSE_TOPIC",
	)

	if err != nil {
		err = errors.Wrapf(
			err,
			"Env-var %s is required for testing, but is not set", missingVar,
		)
		log.Fatalln(err)
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "InventoryAggregate Suite")
}

var _ = Describe("InventoryAggregate", func() {
	var (
		kafkaBrokers          []string
		eventsTopic           string
		producerResponseTopic string

		mockInv   *inventory.Inventory
		mockEvent *model.Event
	)

	BeforeSuite(func() {
		kafkaBrokers = *commonutil.ParseHosts(
			os.Getenv("KAFKA_BROKERS"),
		)
		eventsTopic = os.Getenv("KAFKA_PRODUCER_EVENT_TOPIC")
		producerResponseTopic = os.Getenv("KAFKA_PRODUCER_RESPONSE_TOPIC")

		itemID, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		deviceID, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		rsCustomerID, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())

		mockInv = &inventory.Inventory{
			ItemID:       itemID,
			DateArrived:  time.Now().Unix(),
			DeviceID:     deviceID,
			Lot:          "test-lot",
			Name:         "test-name",
			Origin:       "test-origin",
			Price:        13.4,
			RSCustomerID: rsCustomerID,
			// SalePrice:    12.23,
			SKU:          "test-sku",
			Timestamp:    time.Now().Unix(),
			TotalWeight:  300,
			UPC:          "123456789012",
			WasteWeight:  12,
		}
		marshalInv, err := json.Marshal(mockInv)
		Expect(err).ToNot(HaveOccurred())

		cid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		uid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		uuid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		mockEvent = &model.Event{
			EventAction:   "insert",
			CorrelationID: cid,
			AggregateID:   inventory.AggregateID,
			Data:          marshalInv,
			NanoTime:      time.Now().UnixNano(),
			UserUUID:      uid,
			UUID:          uuid,
			Version:       0,
			YearBucket:    2018,
		}
	})

	Describe("Inventory Operations", func() {
		It("should query record", func(done Done) {
			Byf("Producing MockEvent")
			p, err := kafka.NewProducer(&kafka.ProducerConfig{
				KafkaBrokers: kafkaBrokers,
			})
			Expect(err).ToNot(HaveOccurred())
			marshalEvent, err := json.Marshal(mockEvent)
			Expect(err).ToNot(HaveOccurred())
			p.Input() <- kafka.CreateMessage(eventsTopic, marshalEvent)

			Byf("Creating query args")
			queryArgs := map[string]interface{}{
				"itemID": mockInv.ItemID,
			}
			marshalQuery, err := json.Marshal(queryArgs)
			Expect(err).ToNot(HaveOccurred())

			Byf("Creating query MockEvent")
			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockEvent.EventAction = "query"
			mockEvent.Data = marshalQuery
			mockEvent.NanoTime = time.Now().UnixNano()
			mockEvent.UUID = uuid

			Byf("Producing MockEvent")
			p, err = kafka.NewProducer(&kafka.ProducerConfig{
				KafkaBrokers: kafkaBrokers,
			})
			Expect(err).ToNot(HaveOccurred())
			marshalEvent, err = json.Marshal(mockEvent)
			Expect(err).ToNot(HaveOccurred())
			p.Input() <- kafka.CreateMessage(eventsTopic, marshalEvent)

			// Check if MockEvent was processed correctly
			Byf("Consuming Result")
			c, err := kafka.NewConsumer(&kafka.ConsumerConfig{
				KafkaBrokers: kafkaBrokers,
				GroupName:    "agginv.test.group.1",
				Topics:       []string{producerResponseTopic},
			})
			Expect(err).ToNot(HaveOccurred())
			msgCallback := func(msg *sarama.ConsumerMessage) bool {
				defer GinkgoRecover()
				kr := &model.KafkaResponse{}
				err := json.Unmarshal(msg.Value, kr)
				Expect(err).ToNot(HaveOccurred())

				if kr.UUID == mockEvent.UUID {
					Expect(kr.Error).To(BeEmpty())
					Expect(kr.ErrorCode).To(BeZero())
					Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
					Expect(kr.UUID).To(Equal(mockEvent.UUID))

					result := []inventory.Inventory{}
					err = json.Unmarshal(kr.Result, &result)
					Expect(err).ToNot(HaveOccurred())

					for _, r := range result {
						if r.ItemID == mockInv.ItemID {
							mockInv.ID = r.ID
							Expect(r).To(Equal(*mockInv))
							return true
						}
					}
				}
				return false
			}

			handler := &msgHandler{msgCallback}
			c.Consume(context.Background(), handler)
			Expect(err).ToNot(HaveOccurred())

			close(done)
		}, 20)

		It("should query record with given timestamp", func(done Done) {
			Byf("Producing MockEvent")
			p, err := kafka.NewProducer(&kafka.ProducerConfig{
				KafkaBrokers: kafkaBrokers,
			})
			Expect(err).ToNot(HaveOccurred())
			marshalEvent, err := json.Marshal(mockEvent)
			Expect(err).ToNot(HaveOccurred())
			p.Input() <- kafka.CreateMessage(eventsTopic, marshalEvent)

			Byf("Creating query args")
			mockTime := time.Unix(mockInv.Timestamp, 0)
			queryArgs := inventory.TimeConstraints{
				Start: mockTime.Add(-5 * time.Second).Unix(),
				End:   mockTime.Add(5 * time.Second).Unix(),
			}
			marshalQuery, err := json.Marshal(queryArgs)
			Expect(err).ToNot(HaveOccurred())

			Byf("Creating query MockEvent")
			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockEvent.EventAction = "query"
			mockEvent.ServiceAction = "timestamp"
			mockEvent.Data = marshalQuery
			mockEvent.NanoTime = time.Now().UnixNano()
			mockEvent.UUID = uuid

			Byf("Producing MockEvent")
			p, err = kafka.NewProducer(&kafka.ProducerConfig{
				KafkaBrokers: kafkaBrokers,
			})
			Expect(err).ToNot(HaveOccurred())
			marshalEvent, err = json.Marshal(mockEvent)
			Expect(err).ToNot(HaveOccurred())
			p.Input() <- kafka.CreateMessage(eventsTopic, marshalEvent)

			// Check if MockEvent was processed correctly
			Byf("Consuming Result")
			c, err := kafka.NewConsumer(&kafka.ConsumerConfig{
				KafkaBrokers: kafkaBrokers,
				GroupName:    "agginv.test.group.1",
				Topics:       []string{producerResponseTopic},
			})
			Expect(err).ToNot(HaveOccurred())
			msgCallback := func(msg *sarama.ConsumerMessage) bool {
				defer GinkgoRecover()
				kr := &model.KafkaResponse{}
				err := json.Unmarshal(msg.Value, kr)
				Expect(err).ToNot(HaveOccurred())

				if kr.UUID == mockEvent.UUID {
					Expect(kr.Error).To(BeEmpty())
					Expect(kr.ErrorCode).To(BeZero())
					Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
					Expect(kr.UUID).To(Equal(mockEvent.UUID))

					result := []inventory.Inventory{}
					err = json.Unmarshal(kr.Result, &result)
					Expect(err).ToNot(HaveOccurred())

					for _, r := range result {
						if r.ItemID == mockInv.ItemID {
							mockInv.ID = r.ID
							Expect(r).To(Equal(*mockInv))
							return true
						}
					}
				}
				return false
			}

			handler := &msgHandler{msgCallback}
			err = c.Consume(context.Background(), handler)
			Expect(err).ToNot(HaveOccurred())

			close(done)
		}, 20)

		It("should query record with given count", func(done Done) {
			Byf("Producing MockEvent")
			p, err := kafka.NewProducer(&kafka.ProducerConfig{
				KafkaBrokers: kafkaBrokers,
			})
			Expect(err).ToNot(HaveOccurred())
			marshalEvent, err := json.Marshal(mockEvent)
			Expect(err).ToNot(HaveOccurred())
			p.Input() <- kafka.CreateMessage(eventsTopic, marshalEvent)

			Byf("Creating query args")
			// Get 3 records
			query := &inventory.CountQuery{
				Count: 2,
			}
			marshalQuery, err := json.Marshal(query)
			Expect(err).ToNot(HaveOccurred())

			Byf("Creating query MockEvent")
			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockEvent.EventAction = "query"
			mockEvent.ServiceAction = "count"
			mockEvent.Data = marshalQuery
			mockEvent.NanoTime = time.Now().UnixNano()
			mockEvent.UUID = uuid

			Byf("Producing MockEvent")
			p, err = kafka.NewProducer(&kafka.ProducerConfig{
				KafkaBrokers: kafkaBrokers,
			})
			Expect(err).ToNot(HaveOccurred())
			marshalEvent, err = json.Marshal(mockEvent)
			Expect(err).ToNot(HaveOccurred())
			p.Input() <- kafka.CreateMessage(eventsTopic, marshalEvent)

			// Check if MockEvent was processed correctly
			Byf("Consuming Result")
			c, err := kafka.NewConsumer(&kafka.ConsumerConfig{
				KafkaBrokers: kafkaBrokers,
				GroupName:    "agginv.test.group.1",
				Topics:       []string{producerResponseTopic},
			})
			Expect(err).ToNot(HaveOccurred())
			msgCallback := func(msg *sarama.ConsumerMessage) bool {
				defer GinkgoRecover()
				kr := &model.KafkaResponse{}
				err := json.Unmarshal(msg.Value, kr)
				Expect(err).ToNot(HaveOccurred())

				if kr.UUID == mockEvent.UUID {
					Expect(kr.Error).To(BeEmpty())
					Expect(kr.ErrorCode).To(BeZero())
					Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
					Expect(kr.UUID).To(Equal(mockEvent.UUID))

					result := []inventory.Inventory{}
					err = json.Unmarshal(kr.Result, &result)
					Expect(err).ToNot(HaveOccurred())
					Expect(result).ToNot(BeEmpty())
					return true
				}
				return false
			}

			handler := &msgHandler{msgCallback}
			err = c.Consume(context.Background(), handler)
			Expect(err).ToNot(HaveOccurred())

			close(done)
		}, 20)
	})
})
