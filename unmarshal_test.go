package ddbjson

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

type avTestDef struct {
	filename string
	expected types.AttributeValue
}

var avTestValues = []avTestDef{
	{
		filename: "av_binary",
		expected: &types.AttributeValueMemberB{Value: []byte{'a', 'b', 'c', 'd'}},
	},
	{
		filename: "av_binary_set",
		expected: &types.AttributeValueMemberBS{Value: [][]byte{{'a', 'b', 'c', 'd'}, {'e', 'f', 'g', 'h'}, {'i', 'j', 'k', 'l'}}},
	},
	{
		filename: "av_bool",
		expected: &types.AttributeValueMemberBOOL{Value: true},
	},
	{
		filename: "av_string",
		expected: &types.AttributeValueMemberS{
			Value: "this is a string value",
		},
	},
	{
		filename: "av_string_set",
		expected: &types.AttributeValueMemberSS{
			Value: []string{"one", "two", "three"},
		},
	},
	{
		filename: "av_number",
		expected: &types.AttributeValueMemberN{
			Value: "3.14159",
		},
	},
	{
		filename: "av_number_set",
		expected: &types.AttributeValueMemberNS{
			Value: []string{"1", "2", "3", "4"},
		},
	},
	{
		filename: "av_null",
		expected: &types.AttributeValueMemberNULL{
			Value: true,
		},
	},
}

func TestBasicAvTypes(t *testing.T) {
	for _, data := range avTestValues {
		t.Run(data.filename, func(t *testing.T) {
			rawData, err := os.ReadFile(fmt.Sprintf("test_json%s%s.json", string(os.PathSeparator), data.filename))
			if err != nil {
				t.Error(err)
				return
			}

			if v, err := unmarshalRawDdbValue(rawData); err != nil {
				t.Error(err)
			} else {
				assert.Equal(t, data.expected, v)
			}
		})
	}
}
