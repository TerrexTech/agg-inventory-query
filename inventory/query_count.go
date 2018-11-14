package inventory

import (
	"encoding/json"
	"log"

	"github.com/mongodb/mongo-go-driver/mongo/findopt"

	"github.com/pkg/errors"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
)

type CountQuery struct {
	Count int `json:"count,omitempty"`
}

func queryCount(collection *mongo.Collection, event *model.Event) *model.KafkaResponse {
	countQuery := &CountQuery{}
	err := json.Unmarshal(event.Data, countQuery)
	if err != nil {
		err = errors.Wrap(err, "QueryCount: Error unmarshalling paramters")
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

	if countQuery.Count == 0 {
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
	if countQuery.Count > 100 {
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

	findopts := findopt.Limit(int64(countQuery.Count))
	return queryInventory(collection, event, findopts)
}
