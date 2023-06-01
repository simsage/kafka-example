package nz.test

import java.util.*


fun main() {
    val server = "esb:9092"         // kafka server CSV

    // the client-name must be unique for this to work at all.  The filter will direct to the right processor
    val s1 = setUpStream(topic, uniqueClientName = "converter-1", keyFilter = "converter-", server)
    s1.start()

    // do something else - just wait in this case
    Thread.sleep(900_000L)

    // done - close all
    s1.close()
}

