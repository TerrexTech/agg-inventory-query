package inventory

import (
	"log"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
)

// Query handles "query" events.
func Query(collection *mongo.Collection, event *model.Event) *model.KafkaResponse {
	log.Println("^^^^^^^^^^^^^^^^^^^^^^^^^")
	switch event.ServiceAction {
	case "timestamp":
		return queryTimestamp(collection, event)
	case "count":
		return queryCount(collection, event)
	case "flashOrSale":
		return queryFlashOrSale(collection, event)
	default:
		return queryInventory(collection, event)
	}
}
