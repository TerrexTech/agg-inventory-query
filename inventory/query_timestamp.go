package inventory

import (
	"encoding/json"
	"log"

	"github.com/mongodb/mongo-go-driver/mongo/findopt"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/pkg/errors"
)

// TimeConstraints is the parameter for querying inventory by stat-time and end-time.
type TimeConstraints struct {
	Start int64 `json:"start,omitempty"`
	End   int64 `json:"end,omitempty"`
	Count int   `json:"count,omitempty"`
}

func queryTimestamp(collection *mongo.Collection, event *model.Event) *model.KafkaResponse {
	timeCtnt := &TimeConstraints{}
	err := json.Unmarshal(event.Data, &timeCtnt)
	if err != nil {
		err = errors.Wrap(err, "Query: Error while unmarshalling Event-data")
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

	if timeCtnt.Start == 0 {
		err = errors.New("blank start-time provided")
		err = errors.Wrap(err, "QueryTimestamp")
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

	if timeCtnt.End == 0 {
		err = errors.New("blank end-time provided")
		err = errors.Wrap(err, "QueryTimestamp")
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

	filter := map[string]interface{}{
		"timestamp": map[string]interface{}{
			"$gt": timeCtnt.Start,
			"$lt": timeCtnt.End,
		},
	}
	eventData, err := json.Marshal(filter)
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

	count := timeCtnt.Count
	// if count < 1 {
	// 	count = 50
	// } else if count == 50 {
	// 	count = 50
	// } else if count == 100 {
	// 	count = 100
	// }
	findopts := findopt.Limit(int64(count))
	return queryInventory(collection, event, findopts)
}
