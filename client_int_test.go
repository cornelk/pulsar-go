//go:build integration

package pulsar

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClient(t *testing.T) {
	cases := map[string]struct {
		serverURL   string
		expectedURL string
	}{
		"full": {
			serverURL:   "pulsar://example.com:12345",
			expectedURL: "example.com:12345",
		},
		"host and port": {
			serverURL:   "example.com:12345",
			expectedURL: "example.com:12345",
		},
		"host only": {
			serverURL:   "example.com",
			expectedURL: "example.com:6650",
		},
		"port only": {
			// valid because this will connect to the local host
			serverURL:   ":12345",
			expectedURL: ":12345",
		},
		"empty": {
			serverURL:   "",
			expectedURL: ":6650",
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			client, err := NewClient(c.serverURL)
			require.NoError(t, err)
			assert.Equal(t, c.expectedURL, client.host)
		})
	}
}

func TestClientTopics(t *testing.T) {
	client := setup(t)
	defer func() {
		assert.Nil(t, client.Close())
	}()

	topic := randomTopicName()
	prodConf := ProducerConfig{
		Topic: topic,
		Name:  "test-producer",
	}

	ctx := context.Background()
	_, err := client.NewProducer(ctx, prodConf)
	require.NoError(t, err)

	topics, err := client.Topics(DefaultNamespace)
	require.NoError(t, err)

	var found bool
	for _, t := range topics {
		if t.LocalName == topic {
			found = true
			break
		}
	}

	assert.True(t, found, "topic not found in list")
}
