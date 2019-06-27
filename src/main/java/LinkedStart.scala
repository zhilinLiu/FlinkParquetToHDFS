import java.util

import Table.{ROW, WordCount}
import org.apache.flink.api.common.functions.MapFunction
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
      * val readPath = "hdfs://192.168.2.51:8020/user/flink/source"
      * val checkPointPath = "hdfs://192.168.2.51:8020/user/flink/checkpoint"
      * val writePath = "hdfs://192.168.2.51:8020/apps/hive/warehouse/test/"
      */
    val readPath = "d://wcresult"
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
    evn.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    evn.getCheckpointConfig.setCheckpointTimeout(60000)
    //  最多有一个检查点
    evn.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    evn.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //  检查点保存路径
    evn.setStateBackend(new FsStateBackend(checkPointPath))
    /**
      * 开始操作
      */
    val stream = evn.readFile(format,readPath,FileProcessingMode.PROCESS_CONTINUOUSLY,5)
    val result = stream.map(new MapFunction[String,ROW] {
          override def map(value: String): ROW = {
            val s=value.split(",")
            new ROW(
              s(0),
              s(1).toInt,
              s(2).toInt,
              s(3).toInt,
              s(4),
              s(5),
              s(6).toInt,
              s(7),
              s(8),
              s(9),
              s(10),
              s(11),
              s(12),
              s(13),
              s(14).toInt,
              s(15),
              s(16),
              s(17),
              s(18),
              s(19),
              s(20),
              s(21),
              s(22),
              s(23),
              s(24),
              s(25),
              s(26),
              s(27),
              s(28),
              s(29),
              s(30),
              s(31),
              s(32),
              s(33),
              s(34),
              s(35),
              s(36).toInt,
              s(37).toInt,
              s(38),
              s(39),
              s(40).toInt,
              s(41).toInt,
              s(42).toInt,
              s(43),
              s(44),
              s(45),
              s(46),
              s(47))
          }
        }).keyBy("calling")
        .timeWindow(Time.milliseconds(1000))
        .sum("pay_unit")
    result.print()


    /**
      * 执行
      */
    evn.execute("myjob")

  }
}
