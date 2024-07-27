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

func unmarshalRawDdbValue(value []byte) (types.AttributeValue, error) {
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
			av := types.AttributeValueMemberL{}
			content = &av
			err = json.Unmarshal(v, &av.Value)

		case "M":
			av := types.AttributeValueMemberM{}
			content = &av
			err = json.Unmarshal(v, &av.Value)

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
