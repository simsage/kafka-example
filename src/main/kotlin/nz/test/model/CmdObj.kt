package nz.test.model

import java.io.Serializable
import java.text.SimpleDateFormat
import java.util.*


/**
 * an example command object with some fake properties for transmission over json
 *
 */
class CmdObj(): Serializable {
    var version = ""
    var type = ""
    var time = 0L

    constructor(version: String, type: String, t0: Long) : this() {
        this.version = version
        this.type = type
        this.time = t0
    }

    override fun toString(): String {
        return if (time != 0L)
                    "$version::$type::${sdf.format(Date(time))}"
                else
                    "null"
    }

    companion object {
        private val sdf = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    }

}

