package inventory

import (
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
)

// Query handles "query" events.
func Query(collection *mongo.Collection, event *model.Event) *model.KafkaResponse {
	switch event.ServiceAction {
	case "timestamp":
		return queryTimestamp(collection, event)
	case "count":
		return queryCount(collection, event)
	default:
		return queryInventory(collection, event)
	}
}
