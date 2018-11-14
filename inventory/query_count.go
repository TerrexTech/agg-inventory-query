package inventory

import (
	"encoding/json"
	"log"
	"strconv"

	"github.com/mongodb/mongo-go-driver/mongo/findopt"

	"github.com/pkg/errors"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
)

func queryCount(collection *mongo.Collection, event *model.Event) *model.KafkaResponse {
	count, err := strconv.Atoi(string(event.Data))
	if err != nil {
		err = errors.Wrap(err, "QueryCount: Error converting parameter to number")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	if count == 0 {
		err = errors.New("count must be greater than 0")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     DatabaseError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}
	if count > 100 {
		err = errors.New("count must be less than 100")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     DatabaseError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}

	params := map[string]interface{}{
		"timestamp": map[string]interface{}{
			"$ne": 0,
		},
	}
	eventData, err := json.Marshal(params)
	if err != nil {
		err = errors.Wrap(err, "Error marshalling TimeConstraint-filter")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			EventAction:   event.EventAction,
			ServiceAction: event.ServiceAction,
			UUID:          event.UUID,
		}
	}
	event.Data = eventData

	findopts := findopt.Limit(int64(count))
	return queryInventory(collection, event, findopts)
}
