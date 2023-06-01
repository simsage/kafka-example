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
import kotlin.math.absoluteValue


const val t0 = 1000011100L
const val sharedAppName = "SharedAppName7"
const val topic = "SimSageTest7"

/**
 * set up a stream on a topic with a "name" and a filter (startsWith string filter) and produce new records optionally
 *
 * @param topic the topic
 */
fun setUpStream(topic: String, uniqueClientName: String, keyFilter: String, server: String): KafkaStreams {
    val builder = StreamsBuilder()
    val graph = builder.stream(
        topic,
        Consumed.with(Serdes.String(), Serdes.serdeFrom(KafkaPayloadSerializer(), KafkaPayloadDeserializer()))
    )
    graph.filter{ k, _ -> k.startsWith(keyFilter) }.foreach { k, v ->
        run {
            if (v != null && v is CmdObj) {
                if (v.time == t0) {
                    println("$uniqueClientName received: key: $k, value: $v")
                }
            }
        }
    }

    val streamSettings = Properties()
    streamSettings[StreamsConfig.APPLICATION_ID_CONFIG] = sharedAppName
    streamSettings[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = server
    streamSettings[StreamsConfig.EXACTLY_ONCE_V2] = "true"

    return KafkaStreams(builder.build(streamSettings), streamSettings)
}

// for gradle :run
class Main

fun main() {
    val server = "esb:9092"         // kafka server CSV

    // create a producer with ACK = "all"
    val producer = KafkaUtility.createKafkaProducer(simSageNodeName = "SimSageNode1", server)

    // send 10 messages to the topic
    println("sending 10 messages")
    for (i in 0 until 10) {
        val rndInt = Random().nextInt().absoluteValue
        val f1 = producer.send(ProducerRecord(topic, "converter-$rndInt", CmdObj(i.toString(), "convert", t0)))
        while (!f1.isDone)
            Thread.sleep(100)
    }

    // and start a set of streams listening for exact keys
    println("sending done, now listening for 120 seconds")

    // do something else - just wait in this case
    Thread.sleep(120_000L)
    producer.close()
}

