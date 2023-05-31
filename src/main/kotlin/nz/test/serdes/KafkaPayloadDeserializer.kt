package nz.test.serdes

import org.apache.kafka.common.serialization.Deserializer


/**
 * enable Kafka to stream java objects
 *
 */
class KafkaPayloadDeserializer : Deserializer<Any?> {

    override fun configure(map: Map<String, *>?, b: Boolean) {
        // no need to configure here
    }

    override fun deserialize(topic: String?, data: ByteArray?): Any? {
        if (data == null)
            return null
        return JsonSerDes.objectFromBytes(data)
    }

    override fun close() {
        // no need to close
    }
}
