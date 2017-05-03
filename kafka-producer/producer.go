
package main

import (
    "github.com/Shopify/sarama"
    "log"
    "os"
    "strings"
)

 

var (
    logger = log.New(os.Stderr, "[srama]", log.LstdFlags)
)


func main() {
    sarama.Logger = logger
    config := sarama.NewConfig()
    config.Producer.RequiredAcks = sarama.WaitForAll
    config.Producer.Partitioner = sarama.NewRandomPartitioner
    config.Producer.Return.Successes = true
    msg := &sarama.ProducerMessage{}
    msg.Topic = "hello"
    msg.Partition = int32(-1)
    msg.Key = sarama.StringEncoder("key")
    msg.Value = sarama.ByteEncoder("aa bb cc")
    producer, err := sarama.NewSyncProducer(strings.Split("192.168.99.111:9092", ","), config)
    if err != nil {
        logger.Println("Failed to produce message: %s", err)
        os.Exit(500)
    }
    defer producer.Close()
    partition, offset, err := producer.SendMessage(msg)
    if err != nil {
        logger.Println("Failed to produce message: ", err)
    }
    logger.Printf("partition=%d, offset=%d\n", partition, offset)
}
