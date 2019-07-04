import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.io.FilePathFilter
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object test {
  def main(args: Array[String]): Unit = {
    val schema1 = "{\"namespace\":\"flinkRun\"," +
      "             \"type\": \"record\"," +
      "             \"name\": \"parquet_test\"," +
      "             \"fields\": [{\"name\": \"name\", \"type\": [\"string\",\"null\"]}," +
      "                          {\"name\": \"id\",\"type\": [\"int\",\"null\"]}" +
      "             ]}  "
    val schema: Schema = new Schema.Parser().parse(schema1)
    val readPath = "d://wordcount"
    val checkPointPath = "file:///d://checkpoint"
    val writePath = "d://b"
    val format = new TextInputFormat(new Path(readPath))
    format.setFilesFilter(FilePathFilter.createDefaultFilter())
    format.setCharsetName("utf-8")
    val typeInfo = BasicTypeInfo.STRING_TYPE_INFO
    /**
      * 环境配置
      */
    val evn = StreamExecutionEnvironment.getExecutionEnvironment
    //  开启检查点
    evn.enableCheckpointing(1000)
    //  准确一次语义
    evn.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //  设置检查点之间的间隔
    evn.getCheckpointConfig.setMinPauseBetweenCheckpoints(5000)
    evn.getCheckpointConfig.setCheckpointTimeout(60000)
    //  最多有一个检查点
    evn.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    evn.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //  检查点保存路径
    evn.setStateBackend(new FsStateBackend(checkPointPath))
    /**
      * 开始操作
      */
    val stream = evn.readFile(format,readPath,FileProcessingMode.PROCESS_CONTINUOUSLY,1)
    val result = stream.map((_,1)).keyBy(0).timeWindow(Time.seconds(1)).sum(1)
    result.addSink(new ParquetSink[(String, Int)]("file:///","d://wordcount",schema1,1000) {
      override def putValue(gr: GenericRecord, v: (String, Int)): Unit = {
        gr.put("name",v._1)
        gr.put("id",v._2)
      }
    })
    evn.execute()
  }
}
