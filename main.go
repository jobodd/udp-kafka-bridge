package main

import (
    "fmt"
    "log"
    "net"
    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)


// func sendResponse(conn *net.UDPConn, addr *net.UDPAddr) {
func sendResponse(buf []byte, p *kafka.Producer) {

    topic := "simulator";
    p.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{
            Topic: &topic,
            Partition: kafka.PartitionAny,
        },
        Value:          buf,
    }, nil)

    fmt.Println(string(buf))

}


//NOTE: 0.1;vCar,123;gLat,456;gLong,789

var bootstrap =  "10.1.85.195:9094,10.1.85.196:9094,10.1.85.197:9094"
func main() {
    producerPtr, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrap})
    if err != nil {
        panic(err)
    }

    defer producerPtr.Close()

    // clearTopic("simulator", bootstrap)

    fmt.Println("Kafka producer setup complete")

    fmt.Println("Listening for UDP")

    p := make([]byte, 2048)
    addr := net.UDPAddr{
        Port: 1234,
        IP: net.ParseIP("127.0.0.1"),
    }
    ser, err := net.ListenUDP("udp", &addr)
    if err != nil {
        fmt.Printf("Some error %v\n", err)
        return
    }

    // start a listener for kafka events to log whether we have
    // a delivery success or fail
    go func() {
        for e := range producerPtr.Events() {
            switch ev := e.(type) {
            case *kafka.Message:
                if ev.TopicPartition.Error != nil {
                    fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
                } else {
                    fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
                }
            }
        }
    }()

    // loop forever listening to incoming UDP packets
    for {
        n,remoteaddr,err := ser.ReadFromUDP(p)
        fmt.Printf("Read a message from %v %s \n", remoteaddr, p)
        if err !=  nil {
            fmt.Printf("Some error  %v", err)
            continue
        }
        _,err = ser.WriteToUDP([]byte("From server: Hello I got your message "), remoteaddr)
        go sendResponse(p[:n], producerPtr)
    }
}


func clearTopic(topic string, bootstrapServers string) {
    c, err := kafka.NewConsumer(&kafka.ConfigMap{
        "bootstrap.servers": bootstrapServers,
        "group.id":          "clear-group",
        "auto.offset.reset": "earliest",
    })
    if err != nil {
        log.Fatalf("Failed to create consumer: %s\n", err)
    }
    defer c.Close()
    
    c.SubscribeTopics([]string{topic}, nil)
    log.Println("Consuming all messages from the topic to clear it...")
    
    for {
        msg, err := c.ReadMessage(-1)
        if err != nil {
            if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrPartitionEOF {
                break // End of partition, topic is "cleared"
            }
            log.Printf("Error reading message: %s\n", err)
        } else {
            log.Printf("Consumed message: %s\n", msg.Value)
        }
    }
}

