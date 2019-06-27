import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.flink.api.common.io.FilePathFilter
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.{CheckpointConfig, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.windowing.time.Time
object LinkedStart {

  def main(args: Array[String]): Unit = {

    /**
      * 参数
      */
    val readPath = "hdfs://192.168.2.51:8020/user/flink/source"
    val checkPointPath = "hdfs://192.168.2.51:8020/user/flink/checkpoint"
    val format = new TextInputFormat(new Path(readPath))
    format.setFilesFilter(FilePathFilter.createDefaultFilter())
    format.setCharsetName("utf-8")
    val typeInfo = BasicTypeInfo.STRING_TYPE_INFO
    /**
      * 环境配置
      */
    val evn = StreamExecutionEnvironment.getExecutionEnvironment
    evn.enableCheckpointing(1000)
    evn.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    evn.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    evn.getCheckpointConfig.setCheckpointTimeout(60000)
    evn.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    evn.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    evn.setStateBackend(new FsStateBackend(checkPointPath))
    /**
      * 开始操作
      */
    val text = evn.readFile(format,readPath,FileProcessingMode.PROCESS_CONTINUOUSLY,5)

    evn.execute()

  }
}
