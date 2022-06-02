//go:build integration

package pulsar

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClientTopicsCommunication(t *testing.T) {
	client, err := NewClient("pulsar://localhost:6650",
		WithLogger(newTestLogger(t)),
		withDialer(defaultDialer))
	require.NoError(t, err)
	require.NotNil(t, client)

	/*
			TODO simulate communication
		=== RUN   TestClientTopics
		log.go:184: TestClientTopics 2021/10/01 01:08:52 conn.go:72: *** Sending command: type:CONNECT connect:<client_version:"Pulsar Go 0.01" auth_method_name:"" protocol_version:15 >
		log.go:184: TestClientTopics 2021/10/01 01:08:52 client.go:407: *** Received command: type:CONNECTED connected:<server_version:"Pulsar Server" protocol_version:15 max_message_size:5242880 >
		log.go:184: TestClientTopics 2021/10/01 01:08:52 conn.go:72: *** Sending command: type:PARTITIONED_METADATA partitionMetadata:<topic:"persistent://public/default/topic-qinMLiUv" request_id:1 >
		log.go:184: TestClientTopics 2021/10/01 01:08:52 client.go:407: *** Received command: type:PARTITIONED_METADATA_RESPONSE partitionMetadataResponse:<request_id:1 response:Failed error:MetadataError message:"Policies not found for public/default namespace" >
		log.go:184: TestClientTopics 2021/10/01 01:08:52 conn.go:72: *** Sending command: type:LOOKUP lookupTopic:<topic:"persistent://public/default/topic-qinMLiUv" request_id:2 authoritative:false >
		log.go:184: TestClientTopics 2021/10/01 01:08:52 client.go:407: *** Received command: type:LOOKUP_RESPONSE lookupTopicResponse:<response:Failed request_id:2 error:MetadataError message:"org.apache.pulsar.broker.web.RestException: Policies not found for public/default namespace" >
		log.go:184: TestClientTopics 2021/10/01 01:08:52 client.go:401: Processing received command type:LOOKUP_RESPONSE lookupTopicResponse:<response:Failed request_id:2 error:MetadataError message:"org.apache.pulsar.broker.web.RestException: Policies not found for public/default namespace" >  failed: %!w(*errors.errorString=&{topic lookup response not supported: Failed})
		log.go:184: TestClientTopics 2021/10/01 01:09:22 client.go:407: *** Received command: type:PING ping:<>
		log.go:184: TestClientTopics 2021/10/01 01:09:22 conn.go:72: *** Sending command: type:PONG pong:<>
		log.go:184: TestClientTopics 2021/10/01 01:09:52 client.go:407: *** Received command: type:PING ping:<>
		log.go:184: TestClientTopics 2021/10/01 01:09:52 conn.go:72: *** Sending command: type:PONG pong:<>
		log.go:184: TestClientTopics 2021/10/01 01:10:22 client.go:407: *** Received command: type:PING ping:<>
	*/
}
