package nz.test

import nz.test.model.CmdObj
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.streams.StreamsConfig
import java.util.*

object KafkaUtility {

    /**
     * Create a producer
     *
     * @param simSageNodeName   the name for the producer, not related to anything in the received
     *                          but should be unique
     * @param server    the name of a CSV list of servers, e.g. esb:9092
     */
    fun createKafkaProducer(simSageNodeName: String, server: String): KafkaProducer<String, CmdObj> {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = server
        props[ProducerConfig.CLIENT_ID_CONFIG] = simSageNodeName
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = "nz.test.serdes.KafkaPayloadSerializer"
        props[ProducerConfig.ACKS_CONFIG] = "all"
        return KafkaProducer<String, CmdObj>(props)
    }

    /**
     * create the properties for a consumer - commits cannot be auto and are handled by the streaming system itself
     *
     * @param name      a unique name for the client, not used for targeting / routing
     * @param server    the name of a CSV list of servers, e.g. esb:9092
     */
    fun consumerProperties(name: String, server: String): Properties {
        val props = Properties()
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = server
        props[StreamsConfig.APPLICATION_ID_CONFIG] = name // app id makes a unique receiver
        return props
    }

}

