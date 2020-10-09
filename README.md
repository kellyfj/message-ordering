# Pulsar Message Ordering Test

##Intent
To show a possible bug or misunderstanding when using a single Pulsar producer to send a stream of messages.
When using Synchronous send they appear to arrive in order.
When using asynchronous send they appear NOT to arrive in order, or they can as long as the `matchBatchingSize` is set to 1.

#Design
1) Read a WAV file
2) Break it up into separate messages and send it over a Pulsar Producer
3) Create a consumer and read from the same topic
4) Write the received data to a file
5) Compare the input and output file sizes and contents

#To Run
1) Run a local Pulsar using docker e.g.
```
docker run -it \
  -p 6650:6650 \
  -p 8080:8080 \
  --mount source=pulsardata,target=/pulsar/data \
  --mount source=pulsarconf,target=/pulsar/conf \
  apachepulsar/pulsar:2.6.1 \
  bin/pulsar standalone

```

2)  Test with Synchronous Send e.g.
```
./gradlew run --args='sync 1000'
```
Should show
```
Sent a total of 2646044 bytes
Received 2646044 bytes
Output file:  /tmp/wav-1602249261729.wav
File Sizes same? true
File contents same? true
```

3) Test with Asynchronous Send (batchingMaxMessages=default)
Should show
```
Sent a total of 2646044 bytes
Received 2646044 bytes
Output file:  /tmp/wav-1602249425148.wav
File Sizes same? true
File contents same? false
Byte difference 2556116 out of 2646044 total bytes
```

4) Test with Asynchronous Send (batchingMaxMessages=1)
Should show
```
Sent a total of 2646044 bytes
Received 2646044 bytes
Output file:  /tmp/wav-1602249457634.wav
File Sizes same? true
File contents same? true
```

##Upshot
Even though we are using a single producer async send does seem to result in different message ordering