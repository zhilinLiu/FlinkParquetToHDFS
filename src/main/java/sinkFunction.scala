import io.eels.component.parquet.ParquetWriterConfig
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetWriter}
import org.apache.parquet.hadoop.metadata.CompressionCodecName

abstract class sinkFunction[IN](writePath:String,uri:String) extends SinkFunction[IN] with Serializable {
  // parquet文件的约束
  val schema1 = "{\"namespace\":\"flinkRun\"," +
    "             \"type\": \"record\"," +
    "             \"name\": \"parquet_test\"," +
    "             \"fields\": [{\"name\": \"name\", \"type\": [\"string\",\"null\"]}," +
    "                          {\"name\": \"id\",\"type\": [\"int\",\"null\"]}" +
    "             ]}  "

    override def invoke(value: IN): Unit = {
      // 写入parquet文件
        parquetWrite(value)
      println(value.toString)
    }




    def parquetWrite(value:IN):Unit = {

      val schema: Schema = new Schema.Parser().parse(schema1)
      val ccn = CompressionCodecName.SNAPPY
      val config = ParquetWriterConfig()
      val time = System.currentTimeMillis()
      val realPath = uri+writePath+"/"+time+".parquet"
      val writer: ParquetWriter[GenericRecord] = AvroParquetWriter.builder[GenericRecord](new Path(realPath))
        .withSchema(schema)
        .withCompressionCodec(ccn)
        .withPageSize(config.pageSize)
        .withRowGroupSize(config.blockSize)
        .withDictionaryEncoding(config.enableDictionary)
        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
        .withValidation(config.validating)
        .build()
      val gr: GenericRecord = new GenericData.Record(schema)
      putParquetValue(gr,value)
      writer.write(gr)
      writer.close()
      println(time)
    }
    def putParquetValue(gr: GenericRecord,value:IN):Unit
}
