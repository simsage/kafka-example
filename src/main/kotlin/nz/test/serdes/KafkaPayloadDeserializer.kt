package nz.test.serdes
import nz.test.model.CmdObj
import org.apache.kafka.common.serialization.Deserializer


/**
 * enable Kafka to stream java objects
 * simple string + value from byte-array
 */
class KafkaPayloadDeserializer : Deserializer<CmdObj> {

    override fun configure(map: Map<String, *>?, b: Boolean) {}

    override fun deserialize(topic: String?, data: ByteArray?): CmdObj {
        val obj = CmdObj()
        if (data == null)
            return obj
        return try {
            obj.deSerialise(data)
            obj
        } catch (ex: Exception) {
            obj
        }
    }

    override fun close() {}
}

