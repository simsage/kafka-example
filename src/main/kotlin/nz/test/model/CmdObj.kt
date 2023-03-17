package nz.test.model

import java.text.SimpleDateFormat
import java.util.*

class CmdObj() {
    var version = ""
    var type = ""
    var time = 0L

    constructor(version: String, type: String) : this() {
        this.version = version
        this.type = type
        this.time = System.currentTimeMillis()
    }

    override fun toString(): String {
        return if (time != 0L)
                    "$version::$type::${sdf.format(Date(time))}"
                else
                    "null"
    }

    fun serialise(): ByteArray {
        val str = this.toString()
        return str.toByteArray()
    }

    fun deserialise(data: ByteArray): CmdObj {
        if (data.isNotEmpty()) {
            val str = String(data)
            val parts = str.split("::")
            if (parts.size == 3) {
                this.version = parts[0]
                this.type = parts[1]
                this.time = sdf.parse(parts[2]).time
            }
        }
        return this
    }


    companion object {
        private val sdf = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    }

}

