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
type FlashOrSale struct {
	Field  string  `json:"field,omitempty"`
	Weight float64 `json:"weight,omitempty"`
	Count  int     `json:"count,omitempty"`
}

func queryFlashOrSale(collection *mongo.Collection, event *model.Event) *model.KafkaResponse {
	flashOrSale := &FlashOrSale{}
	err := json.Unmarshal(event.Data, &flashOrSale)
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

	if flashOrSale.Weight < 1 {
		err = errors.New("blank weight provided")
		err = errors.Wrap(err, "QueryFlashOrSale")
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
		flashOrSale.Field: map[string]interface{}{
			"$gt": flashOrSale.Weight,
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

	count := flashOrSale.Count
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
