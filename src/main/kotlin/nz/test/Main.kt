package nz.test

import nz.test.model.CmdObj
import nz.test.serdes.KafkaPayloadDeserializer
import nz.test.serdes.KafkaPayloadSerializer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.Consumed
import java.util.*


/**
 * set up a stream on a topic with a "name" and a filter (startsWith string filter) and produce new records optionally
 *
 * @param topic the topic
 */
fun setUpStream(topic: String, uniqueClientName: String, keyFilter: String, t0: Long, server: String): KafkaStreams {
    val builder = StreamsBuilder()
    val graph = builder.stream(
        topic,
        Consumed.with(Serdes.String(), Serdes.serdeFrom(KafkaPayloadSerializer(), KafkaPayloadDeserializer()))
    )
    graph.filter{ k, _ -> k.startsWith("converter-") }.foreach { k, v ->
        run {
            if (v != null && v is CmdObj) {
                if (v.time == t0) {
                    println("$uniqueClientName received: key: $k, value: $v")
                }
            }
        }
    }

    val streamSettings = Properties()
    streamSettings[StreamsConfig.APPLICATION_ID_CONFIG] = "SameNameApp"
    streamSettings[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = server
    // streamSettings[StreamsConfig.EXACTLY_ONCE_V2] = "false" // doesn't seem to do anything

    return KafkaStreams(builder.build(streamSettings), streamSettings)
}

// for gradle :run
class Main

fun main() {
    val topic = "SimSageTest"       // application name - shared between all
    val server = "esb:9092"         // kafka server CSV

    // create a producer with ACK = "all"
    val producer = KafkaUtility.createKafkaProducer(simSageNodeName = "SimSageNode1", server)

    // send 10 messages to the topic
    println("sending 10 messages")
    val t0 = System.currentTimeMillis()
    for (i in 0 until 10) {
        val rndInt = Random().nextInt()
        val f1 = producer.send(ProducerRecord(topic, "converter-$rndInt", CmdObj(i.toString(), "convert", t0)))
        while (!f1.isDone)
            Thread.sleep(100)
    }

    // and start a set of streams listening for exact keys
    println("sending done, now listening for 120 seconds")

    // the client-name must be unique for this to work at all.  The filter will direct to the right processor
    val s1 = setUpStream(topic, uniqueClientName = "converter-0", keyFilter = "converter-0", t0, server)
    s1.start()

    val s2 = setUpStream(topic, uniqueClientName = "converter-1", keyFilter = "converter-0", t0, server)
    s2.start()

    // do something else - just wait in this case
    Thread.sleep(120_000L)

    // done - close all
    s1.close()
    s2.close()
    producer.close()
}

