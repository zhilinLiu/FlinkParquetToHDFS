package Table

import java.net.{URI, URL}

import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.parquet.avro.AvroParquetReader

object test {
  def main(args: Array[String]): Unit = {
//    val hdfsPath = "d://"
//    val hdfs = FileSystem.get(new Configuration())
//    val smallFileList=hdfs.listStatus(new Path("d://b")).filter(x=>(x.getLen<=128*1024*1024))
//    println("小文件个数为:"+smallFileList.size)
//    if(smallFileList.size>1){
//      val outputStream = hdfs.create(new Path("d://b/hechen.parquet"))
//      for (file <- smallFileList){
//        val in = hdfs.open(file.getPath)
//        IOUtils.copyBytes(in,outputStream,4096,false)
//        in.close()
//        hdfs.delete(file.getPath,false)
//        println("写了文件========="+file.getPath.getName)
//      }
//      outputStream.close()
//    }

    val reader = new AvroParquetReader[GenericRecord](new Path("d://b/1561706806774.parquet"))
    var record:GenericRecord = null


    var flag = true
    while (flag){
      record = reader.read()
      if(record==null){
        flag = false
      }else{
        println(record)
      }

    }

  }
}
