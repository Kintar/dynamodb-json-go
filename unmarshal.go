package ddbjson

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type ddbItem struct {
	Item json.RawMessage
}

var ErrInvalidDynamoDbObject = errors.New("invalid DynamoDB object")
var ErrInvalidDynamoDbObjectValue = errors.New("invalid DynamoDb value: failed to unmarshal expected type")

// UnmarshalRawDdbValue attempts to unmarshal the passed byte array into a types.AttributeValue instance.
// The incoming byte array should be valid DynamoDB json.
// If the json appears to be malformed, ErrInvalidDynamoDbObject is returned.
// If the json cannot be unmarshalled into the expected type, ErrInvalidDynamoDbObjectValue is returned
func UnmarshalRawDdbValue(value []byte) (types.AttributeValue, error) {
	var item map[string]json.RawMessage
	err := json.Unmarshal(value, &item)
	if err != nil {
		return nil, fmt.Errorf("%w: failed to unmarshal", ErrInvalidDynamoDbObject)
	}

	if len(item) != 1 {
		return nil, fmt.Errorf("%w: too many keys", ErrInvalidDynamoDbObject)
	}

	var content types.AttributeValue

	for k, v := range item {
		switch k {
		case "B":
			av := types.AttributeValueMemberB{}
			content = &av
			var encoded string
			if err = json.Unmarshal(v, &encoded); err == nil {
				if av.Value, err = base64.StdEncoding.DecodeString(encoded); err != nil {
					err = fmt.Errorf("%w: base64 decode failed", ErrInvalidDynamoDbObjectValue)
				}
			}

		case "BOOL":
			av := types.AttributeValueMemberBOOL{}
			content = &av
			err = json.Unmarshal(v, &av.Value)

		case "BS":
			av := types.AttributeValueMemberBS{}
			content = &av
			var strs []string
			var bs [][]byte
			if err = json.Unmarshal(v, &strs); err == nil {
				for _, str := range strs {
					if b, e2 := base64.StdEncoding.DecodeString(str); e2 == nil {
						bs = append(bs, b)
					} else {
						err = e2
						bs = nil
						break
					}
				}
			}

			av.Value = bs

		case "L":
			var items []json.RawMessage
			var results []types.AttributeValue
			if err = json.Unmarshal(v, &items); err != nil {
				return nil, fmt.Errorf("%w: failed to unmarshal list", ErrInvalidDynamoDbObjectValue)
			}
			for _, item := range items {
				unmarshalledItem, err := UnmarshalRawDdbValue(item)
				if err != nil {
					return nil, fmt.Errorf("%w: failed to unmarshal list item", ErrInvalidDynamoDbObject)
				}
				results = append(results, unmarshalledItem)
			}
			content = &types.AttributeValueMemberL{Value: results}

		case "M":
			var rawMap map[string]json.RawMessage
			if err = json.Unmarshal(v, &rawMap); err != nil {
				return nil, fmt.Errorf("%w: failed to unmarshal map content", ErrInvalidDynamoDbObjectValue)
			}

			results := make(map[string]types.AttributeValue, len(rawMap))
			for k, v := range rawMap {
				data, err := UnmarshalRawDdbValue(v)
				if err != nil {
					return nil, err
				}
				results[k] = data
			}

			content = &types.AttributeValueMemberM{Value: results}

		case "N":
			av := types.AttributeValueMemberN{}
			content = &av
			err = json.Unmarshal(v, &av.Value)

		case "NS":
			av := types.AttributeValueMemberNS{}
			content = &av
			err = json.Unmarshal(v, &av.Value)

		case "NULL":
			av := types.AttributeValueMemberNULL{}
			content = &av
			err = json.Unmarshal(v, &av.Value)

		case "S":
			av := types.AttributeValueMemberS{}
			content = &av
			err = json.Unmarshal(v, &av.Value)

		case "SS":
			av := types.AttributeValueMemberSS{}
			content = &av
			err = json.Unmarshal(v, &av.Value)

		default:
			panic("unhandled default branch")
		}
		if err != nil {
			break
		}
	}

	return content, err
}

// UnmarshalDynamoDbItem attempts to treat the incoming byte array as valid DynamoDB json and convert the contents of
// the "Item" field into a map of dynamoDB AttributeValue members.
// Returns ErrInvalidDynamoDbObject if the object does not have an Item field
// Returns ErrInvalidDynamoDbObject or ErrInvalidDynamoDbObjectValue (depending on the underlying problem) if the item
// body is malformed
func UnmarshalDynamoDbItem(item []byte) (map[string]types.AttributeValue, error) {
	var rawItem ddbItem
	err := json.Unmarshal(item, &rawItem)
	if err != nil {
		return nil, ErrInvalidDynamoDbObject
	}
	var rawMap map[string]json.RawMessage
	err = json.Unmarshal(rawItem.Item, &rawMap)
	if err != nil {
		return nil, ErrInvalidDynamoDbObjectValue
	}

	result := make(map[string]types.AttributeValue)
	for k, v := range rawMap {
		attrValue, err := UnmarshalRawDdbValue(v)
		if err != nil {
			return nil, err
		}
		result[k] = attrValue
	}

	return result, nil
}
