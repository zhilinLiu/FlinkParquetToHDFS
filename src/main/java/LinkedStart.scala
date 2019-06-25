import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.flink.api.common.io.FilePathFilter
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.parquet.hadoop.ParquetWriter
object LinkedStart {

  def main(args: Array[String]): Unit = {
    val schema1 = "{\"namespace\":\"flinkRun\"," +
      "             \"type\": \"record\"," +
      "             \"name\": \"parquet_test\"," +
      "             \"fields\": [{\"name\": \"name\", \"type\": [\"string\",\"null\"]}," +
      "                          {\"name\": \"id\",\"type\": [\"int\",\"null\"]}" +
      "             ]}  "

    val schema = new Schema.Parser().parse(schema1)
    val filePath = "hdfs://192.168.2.51:8020/user/flink/source"
    val format = new TextInputFormat(new Path(filePath))
    format.setFilesFilter(FilePathFilter.createDefaultFilter())
    format.setCharsetName("utf-8")
    val typeInfo = BasicTypeInfo.STRING_TYPE_INFO

    val evn = StreamExecutionEnvironment.getExecutionEnvironment
    evn.enableCheckpointing(1000)
    evn.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    evn.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    evn.getCheckpointConfig.setCheckpointTimeout(60000)
    evn.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    evn.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    evn.setStateBackend(new FsStateBackend("hdfs://192.168.2.51:8020/user/flink/checkpoint"))
    val text = evn.readFile(format,filePath,FileProcessingMode.PROCESS_CONTINUOUSLY,5)
    val result = text.map((_,1)).keyBy(0).timeWindow(Time.milliseconds(100))
      .sum(1)
    result.print()
    result.addSink(new sinkFunction[(String, Int)] {
      override def writeGR(gr: GenericRecord, value: (String, Int)): Unit = {
        gr.put("name",value._1)
        gr.put("id",value._2)
      }
    })
    evn.execute()

  }
}
