package nz.test.serdes

import com.fasterxml.jackson.core.StreamReadConstraints
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper

class JsonSerDes {
    var javaClassName = ""
    var cmdObjStr = ""

    companion object {
        // must match others in link KnowledgeBaseInventoryDao to work
        private const val blockSizeInBytes = 5 * 1024 * 1024

        private val mapper = jacksonObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .registerModule(JavaTimeModule())


        init {
            // allow for string sizes up to blockSizeInBytes x 2 (UTF-16)
            val streamReadConstraints = StreamReadConstraints.builder()
                .maxNestingDepth(StreamReadConstraints.DEFAULT_MAX_DEPTH)
                .maxNumberLength(StreamReadConstraints.DEFAULT_MAX_NUM_LEN)
                .maxStringLength(blockSizeInBytes * 2).build()
            mapper.tokenStreamFactory().setStreamReadConstraints(streamReadConstraints)
        }

        // turn any object into a byte array of a JsonSerDes object serialized
        fun objectToBytes(cmdObj: Any): ByteArray {
            return try {
                val serDes = JsonSerDes()
                serDes.cmdObjStr = mapper.writeValueAsString(cmdObj)
                serDes.javaClassName = cmdObj::class.java.name
                mapper.writeValueAsBytes(serDes)
            } catch (ex: Exception) {
                println("objectToBytes: ${ex.message?:ex}")
                ByteArray(0)
            }
        }


        // turn a byte-array back into a carried object inside the JsonSerDes, or null if not possible
        fun objectFromBytes(data: ByteArray): Any? {
            // read the JsonSerDes object
            return try {
                val obj2 = mapper.readValue(String(data), JsonSerDes::class.java)
                if (obj2.javaClassName.isNotBlank()) {
                    // using the class name inside the ser-des object, read the rest of the json and create the true class
                    mapper.readValue(obj2.cmdObjStr, Class.forName(obj2.javaClassName))
                } else {
                    null
                }
            } catch (ex: Exception) {
                println("objectFromBytes: ${ex.message?:ex}")
                null
            }
        }


    }

}

