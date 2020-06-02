package pulsar

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewTopic(t *testing.T) {
	tests := []struct {
		name         string
		error        string
		completeName string
	}{
		{
			name:         "topic-1",
			completeName: "persistent://public/default/topic-1",
		},
		{
			name:         "property/namespace/topic-1",
			completeName: "persistent://property/namespace/topic-1",
		},
		{
			name:  "namespace/topic-1",
			error: "invalid topic short name format",
		},
		{
			name:  "://tenant://namespace",
			error: "invalid topic domain format",
		},
		{
			name:  "://tenant/namespace",
			error: "invalid topic domain",
		},
		{
			name:  "persistent://property/namespace/topic/1",
			error: "invalid topic name format",
		},
	}

	for _, test := range tests {
		topic, err := newTopic(test.name)
		if err == nil {
			if test.error != "" {
				t.Errorf("%v: unexpected error '%v'", test.name, err)
			}
		} else {
			if test.error != err.Error() {
				t.Errorf("%v: expected error '%v' but got '%v'", test.name, test.error, err)
			}
		}

		if topic == nil {
			continue
		}

		assert.Equal(t, test.completeName, topic.completeTopicName)
	}
}
