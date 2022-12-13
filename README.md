# kafeman

kaf inspired cli for kafka management

переливание данных из одного кластера в другой

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