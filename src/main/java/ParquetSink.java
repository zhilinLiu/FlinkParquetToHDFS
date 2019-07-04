import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.expressions.In;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.io.Serializable;

public abstract class ParquetSink<IN> extends RichSinkFunction<IN> implements Serializable {
    private Contain contain ;
    private String writePath;
    private String uri;
    private int count = 0;
    private String schema;
    private int num;

    /**
     *
     * @param uri           路径前缀，如  hdfs://   file:///
     * @param writePath     路径 如 /user
     * @param schema       json格式的约束
     * @param num   一个parquet文件行数
     */
    public ParquetSink(String uri, String writePath, String schema,int num) {
        this.schema = schema;
        this.writePath = writePath;
        this.uri = uri;
        this.num = num;

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.contain = new Contain(schema);
    }

    @Override
    public void invoke(IN value) throws Exception {
        if(contain.writer!=null){
            if(count<=num){
                GenericRecord gr = new GenericData.Record(contain.schema);
                putValue(gr,value);
                contain.writer.write(gr);
                count++;
            }else {
                contain.writer.close();
                contain.writer=null;
                count=0;
            }
        }else {
            long time = System.currentTimeMillis();
            int random = (int) (Math.random()*1000);
            contain.writer = AvroParquetWriter.builder(new Path(uri+writePath+"/"+time+random+".parquet"))
                    .withSchema(contain.schema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withPageSize(1048576)
                    .withRowGroupSize(128 * 1024 * 1024)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .build();
            GenericRecord gr = new GenericData.Record(contain.schema);
            putValue(gr,value);
            contain.writer.write(gr);
            count++;
        }
        System.out.println(Thread.currentThread().getName()+" "+contain.writer+" "+count);
    }
    public abstract void putValue(GenericRecord gr,IN v);
    class Contain implements Serializable {
        private ParquetWriter<Object> writer;
        private Schema schema ;

        public Contain(String schema) {
            this.schema = new Schema.Parser().parse(schema);
        }
    }
}
