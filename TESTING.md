## How to run the integration tests

Set up a `docker-compose.yml` with following content:

```yaml
version: '3.5'

services:
  pulsar:
    image: apachepulsar/pulsar:2.5.2
    container_name: pulsar_test
    ports:
      - "6650:6650"
      - "9080:8080"
    volumes:
      - ./data/pulsar:/pulsar/data
    command: "bin/pulsar standalone"
```

Start the container: 

`docker-compose up -d`

Run the integration test:

`go test -tags integration -v .`
