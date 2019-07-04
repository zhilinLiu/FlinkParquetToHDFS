# FlinkParquetToHDFS
this project is finding way how to use flink sink to parquetFiles to HDFS.
You can use StreamingEnviroment.addSink method then new ParquetSink,according to the json.schema put your field and value.
like this :
            // argument 1        --- uri   for example : file:///  or  hdfs:// 
            // argument 2        --- path  for example : c://  or   /user/home
            // argument 3        --- schema   it's a json string.
            // argument 4        ---num       a parquet file contain how many rows.
            streaming.addSink(new ParquetSink[(String, Int)]("file:///","d://wordcount",schema1,1000) {
                  override def putValue(gr: GenericRecord, v: (String, Int)): Unit = {
                    gr.put("name",v._1)
                    gr.put("id",v._2)
                  }
                  
