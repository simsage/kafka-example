package nz.test.serdes
import nz.test.model.CmdObj
import org.apache.kafka.common.serialization.Serializer


/**
 * enable Kafka to stream java objects
 * simple string + value to byte-array
 */
class KafkaPayloadSerializer : Serializer<CmdObj> {

    override fun configure(map: Map<String, *>?, b: Boolean) {}

    override fun serialize(s: String, o: CmdObj): ByteArray {
        return try {
            o.serialise()

        } catch (ex: Exception) {
            ByteArray(0)
        }
    }

    override fun close() {}
}

