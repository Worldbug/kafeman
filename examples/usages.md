# Config
For better and compact commands syntax clusters and topics settings separated in config.yaml
located in *.config/kafeman/config.yml*

```yaml
# example config
current_cluster: local
# contains brokers list
clusters:
- name: my_cluster
  brokers:
  - my_broker1:9092
  - my_broker2:9092
  - my_broker3:9092
- name: local
  brokers:
  - localhost:9092
topics:
  # if message in topic is avro-encoded 
  - name: avro_topic
    avro_schema_url: https://my-registry/core-schema
  # if message in topic is proto-encoded
  - name: proto_topic
    proto_type: model.Event
    proto_paths:
    - ~/my_project/protos
```

Other examples of configuration (with auth) you can see in this folder
(they borrowed from kaf)

# Consume
Consume all massages where key equal "my_key"
```sh
kafeman consume topic_name --meta | jq 'select(.key=="my_key")'
```
Consume all massages after timestamp
```sh
kafeman consume topic_name --meta --from 2023-01-21T01:40:03
```
Consume all massages, but not decode
```sh
kafeman consume topic_name --force-encoding raw
```

# Replication
Replicate messages from prod cluster to local cluster
```sh
kafeman replicate prod/my_topic local/my_topic
```

Also you can replicate messages after specific timestamp
```sh
kafeman replicate prod/my_topic local/my_topic --from 2023-01-21T01:40:03
```
Or replicate only one messages per partition
```sh
kafeman replicate prod/my_topic local/my_topic -n 1
```

# Topic describe

Print pretty topic describe 
```sh
kafeman topic describe my_topic
```
Also you can print topic info as json data for filtering with jq
```sh
kafeman topic describe my_topic --json
```

Print only hight watermarks of topic partitions

```sh
kafeman topic describe my_topic --json | jq '.partitions[].hight_watermark'
```
You can print topic consumers 
```sh
kafeman topic consumers

```

If if topic contains proto-encoded messages, and you specify proto model in config, you can print example message in terminal

```sh
kafeman example proto_topic
```

You can generate example of message and pass in vim for edit then produce to topic
```sh
kafeman example proto_topic | vim | kafeman produce proto_topic
```