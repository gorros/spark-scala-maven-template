package com.github.gorros.helpers

import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroWrapper
import com.twitter.bijection.avro.GenericAvroCodecs

object Utils {
    def avroToBytes(aw: AvroWrapper[GenericRecord]): Array[Byte] = {
        GenericAvroCodecs.toBinary(aw.datum().getSchema).apply(aw.datum())
    }
}
