# kafeman
kaf inspired cli for kafka management
* Optimizations for fast performance in large kafka clusters
* Simplified work with proto in topics
* Alternative output in json format
* Work not only with offsets but also with timestamps
* More informative describe

## Install

```sh
go install github.com/wordlbug/kafeman/cmd/kafeman@latest
```
make sure go/bin is in PATH

```sh
export PATH=$PATH:$GOROOT/bin:$GOPATH/bin
```

### completions
Aftrer install you can add completions for shell

Bash:
```sh
# For current session
source <(kafeman completion bash)

# Linux:
kafeman completion bash > /etc/bash_completion.d/kafeman

# MacOS:
kafeman completion bash > /usr/local/etc/bash_completion.d/kafeman
```
Zsh:
```sh
kafeman completion zsh > "${fpath[1]}/_kafeman"
# You will need to start a new shell for this setup to take effect.
```
Fish:
```sh
kafeman completion fish | source
# To load completions for each session, execute once:
kafeman completion fish > ~/.config/fish/completions/kafeman.fish
```
### Config

Before starting, initialize the config
```sh
kafeman config init
```
In the config you can configure which proto type is consumed in which topic
```yaml
current_cluster: prod
clusters:
- name: prod
  brokers:
  - kafkabroker.prod:9092
- name: stg
  brokers:
  - kafkabroker.stg:9092
- name: local
  brokers:
  - localhost:9092
topics:
  my-topic-1:
    proto_type: myTopic.Event
    proto_paths:
    - project/protos
 ```
###


## Status
### Legend
✅ Fully implemented \
❌ Not implemented \
✔️ Partially implemented

### Supported messages types
|state|type|
| :---: | :---: |
✅|json
✅|protobuf
❌|message-pack
❌|avro

### consume

|kafeman|kaf|
| :---: | :---: |
✅| commit
✅| follow
✅| group
✅| help                    
✅| offset
✅| partitions
✅| raw                     
✅| tail
✔️| key-proto-type
✔️| proto-include
✔️| proto-type
❌| limit-messages
❌| proto-exclude 
❌| decode-msgpack

### group
|kafeman|kaf|
|:---:|:---:|
❌| commit
✅| delete
✅| describe
✅| ls
❌| peek

### topic
|kafeman|kaf|
|:---:|:---:|
✅| describe
✅| ls
❌| create
❌| delete
❌| update