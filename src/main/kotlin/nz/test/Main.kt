package nz.test

import nz.test.model.CmdObj
import nz.test.serdes.KafkaPayloadDeserializer
import nz.test.serdes.KafkaPayloadSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed


/**
 * set up a stream on a topic with a "name" and a filter (startsWith string filter) and produce new records optionally
 *
 * @param topic the topic
 */
fun setUpStream(topic: String, uniqueClientName: String, keyFilter: String,
                producer: KafkaProducer<String, CmdObj>?, server: String): KafkaStreams {
    val builder = StreamsBuilder()
    val graph = builder.stream(
        topic,
        Consumed.with(Serdes.String(), Serdes.serdeFrom(KafkaPayloadSerializer(), KafkaPayloadDeserializer()))
    )
    graph.filter { k, _ -> k == keyFilter }.foreach { k, v ->
        run {
            println("$uniqueClientName received: key: $k, value: $v")
            if (producer != null) {
                println("$uniqueClientName sending \"$v\" to return")
                producer.send(ProducerRecord(topic, "return", CmdObj("1", "return value from \"$v\"")))
            }
        }
    }
    return KafkaStreams(builder.build(), KafkaUtility.consumerProperties(uniqueClientName, server))
}


fun main() {
    val topic = "SimSageTest"       // application name - shared between all
    val server = "esb:9092"         // kafka server CSV

    // create a producer with ACK = "all"
    val producer = KafkaUtility.createKafkaProducer(simSageNodeName = "SimSageNode1", server)

    // send two messages to the producer
    println("sending message 1")
    val f1 = producer.send(ProducerRecord(topic, "converter-0", CmdObj("1", "convert")))
    while (!f1.isDone)
        Thread.sleep(100)

    println("sending message 2")
    val f2 = producer.send(ProducerRecord(topic, "language-0", CmdObj("1", "parse")))
    while (!f2.isDone)
        Thread.sleep(100)

    // and start a set of streams listening for exact keys
    println("sending done, now listening for 120 seconds")

    // the client-name must be unique for this to work at all.  The filter will direct to the right processor
    val s1 = setUpStream(topic, uniqueClientName = "converter-0", keyFilter = "converter-0", producer, server)
    s1.start()

    val s2 = setUpStream(topic, uniqueClientName = "language-0", keyFilter = "language-0", producer, server)
    s2.start()

    val s3 = setUpStream(topic, uniqueClientName = "return-0", keyFilter = "return", producer = null, server)
    s3.start()

    // do something else - just wait in this case
    Thread.sleep(120_000L)

    // done - close all
    s1.close()
    s2.close()
    s3.close()
    producer.close()
}

