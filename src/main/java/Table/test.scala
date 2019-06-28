package Table

import java.io.File
import java.net.{URI, URL}

import io.eels.component.parquet.ParquetWriterConfig
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.hadoop.{ParquetFileWriter, ParquetWriter}

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

    val schema1 = "{\"namespace\":\"flinkRun\"," +
      "             \"type\": \"record\"," +
      "             \"name\": \"parquet_test\"," +
      "             \"fields\": [{\"name\": \"name\", \"type\": [\"string\",\"null\"]}," +
      "                          {\"name\": \"id\",\"type\": [\"int\",\"null\"]}" +
      "             ]}  "
    val schema: Schema = new Schema.Parser().parse(schema1)
    val ccn = CompressionCodecName.SNAPPY
    val config = ParquetWriterConfig()
    val writer: ParquetWriter[GenericRecord] = AvroParquetWriter.builder[GenericRecord](new Path("d://all.parquet"))
    .withSchema(schema)
    .withCompressionCodec(ccn)
    .withPageSize(config.pageSize)
    .withRowGroupSize(config.blockSize)
    .withDictionaryEncoding(config.enableDictionary)
    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
    .withValidation(config.validating)
    .build()
    val gr: GenericRecord = new GenericData.Record(schema)
    val files = new File("d://b")
    val filelist = files.listFiles()
    filelist.filter(f=>f.length()<1024*1024*18).foreach{f =>
      val reader = new AvroParquetReader[GenericRecord](new Path(f.getPath))
      var record:GenericRecord = null
      var flag = true
      while (flag){
        record = reader.read()
        if(record==null){
          flag = false
        }else{
          gr.put("name",record.get("name"))
          gr.put("id",record.get("id"))
          try{
          writer.write(gr)

          }catch {
            case e:Exception =>{
              println("报错啦")
            }
          }


          println(record)
        }
    }


    }
    writer.close()
  }
}
