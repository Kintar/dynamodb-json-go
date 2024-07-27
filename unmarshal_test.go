package ddbjson

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"os"
	"strings"
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
	{
		filename: "av_list",
		expected: &types.AttributeValueMemberL{
			Value: []types.AttributeValue{
				&types.AttributeValueMemberNULL{Value: true},
				&types.AttributeValueMemberBOOL{Value: true},
			},
		},
	},
	{
		filename: "av_map",
		expected: &types.AttributeValueMemberM{
			Value: map[string]types.AttributeValue{
				"string":     &types.AttributeValueMemberS{Value: "this is a string value"},
				"string_set": &types.AttributeValueMemberSS{Value: []string{"one", "two", "three"}},
			},
		},
	},
}

func TestBasicAvTypes(t *testing.T) {
	for _, data := range avTestValues {
		t.Run(data.filename, func(t *testing.T) {
			rawData, err := os.ReadFile(fmt.Sprintf("test_json%s%s.json", string(os.PathSeparator), data.filename))
			if err != nil {
				t.Error(err)
				t.FailNow()
			}

			if v, err := UnmarshalRawDdbValue(rawData); err != nil {
				t.Error(err)
			} else {
				assert.Equal(t, data.expected, v)
			}
		})
	}
}

func TestUnmarshalDynamoDbItem(t *testing.T) {
	rawData, err := os.ReadFile(fmt.Sprintf("test_json%s%s.json", string(os.PathSeparator), "ddb_item"))
	if err != nil {
		t.Errorf("could not read ddb_item.json")
		t.FailNow()
	}

	ddbmap, err := UnmarshalDynamoDbItem(rawData)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	assert.Equal(t, len(avTestValues), len(ddbmap))
	for _, v := range avTestValues {
		key := strings.SplitN(v.filename, "_", 2)[1]
		assert.Equalf(t, v.expected, ddbmap[key], "bad value for %s", key)
	}
}
