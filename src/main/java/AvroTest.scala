import io.eels.component.parquet.ParquetWriterConfig
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetWriter}
import org.apache.parquet.hadoop.metadata.CompressionCodecName

object AvroTest {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/hadoop2.7.1")
    //  定义Schema
    val schema1 = "{\"namespace\":\"flinkRun\"," +
      "             \"type\": \"record\"," +
      "             \"name\": \"parquet_test\"," +
      "             \"fields\": [{\"name\": \"name\", \"type\": [\"string\",\"null\"]}," +
      "                          {\"name\": \"id\",\"type\": [\"int\",\"null\"]}" +
      "             ]}  "
    val schema: Schema = new Schema.Parser().parse(schema1)
    //  定义压缩格式
    val ccn = CompressionCodecName.SNAPPY
    // 配置类，包含了一些默认配置
    val config = ParquetWriterConfig()
    val path = "d://re.parquet"
    // G构建writer
    val writer: ParquetWriter[GenericRecord] = AvroParquetWriter.builder[GenericRecord](new Path(path))
      .withSchema(schema)
      .withCompressionCodec(ccn)
      .withPageSize(config.pageSize)
      .withRowGroupSize(config.blockSize)
      .withDictionaryEncoding(config.enableDictionary)
      .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
      .withValidation(config.validating)
      .build()
    //  定义约束和加数据
    val gr: GenericRecord = new GenericData.Record(schema)
    gr.put("name","hh")
    gr.put("id",1)
    writer.write(gr)
    gr.put("name","lzl")
    gr.put("id",2)
    writer.write(gr)
    writer.close()
  }
}
