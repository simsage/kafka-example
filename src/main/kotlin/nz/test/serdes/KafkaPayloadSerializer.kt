package nz.test.serdes

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Serializer

/**
 * enable Kafka to stream java objects
 *
 */
class KafkaPayloadSerializer : Serializer<Any?> {

    override fun configure(map: Map<String, *>?, b: Boolean) {
        // not used here
    }

    override fun serialize(s: String, o: Any?): ByteArray {
        return try {
            if (o != null) {
                return JsonSerDes.objectToBytes(o)
            } else {
                ObjectMapper().writeValueAsBytes(JsonSerDes())
            }
        } catch (ex: Exception) {
            ObjectMapper().writeValueAsBytes(JsonSerDes())
        }
    }

    override fun close() {
        // not used here
    }
}
