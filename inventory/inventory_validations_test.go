package inventory

import (
	"testing"
	"time"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/uuuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// TestInventory only tests basic pre-processing error-checks for Aggregate functions.
func TestInventory(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "InventoryAggregate Suite")
}

var _ = Describe("InventoryAggregate", func() {
	Describe("query", func() {
		It("should return error if filter is empty", func() {
			uuid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			cid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			uid, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			mockEvent := &model.Event{
				EventAction:   "delete",
				CorrelationID: cid,
				AggregateID:   2,
				Data:          []byte("{}"),
				NanoTime:      time.Now().UnixNano(),
				UserUUID:      uid,
				UUID:          uuid,
				Version:       3,
				YearBucket:    2018,
			}
			kr := Query(nil, mockEvent)
			Expect(kr.AggregateID).To(Equal(mockEvent.AggregateID))
			Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
			Expect(kr.Error).ToNot(BeEmpty())
			Expect(kr.ErrorCode).To(Equal(int16(InternalError)))
			Expect(kr.UUID).To(Equal(mockEvent.UUID))
		})
	})
})
