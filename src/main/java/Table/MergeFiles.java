package Table;
import io.eels.component.parquet.ParquetWriterConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class MergeFiles implements Runnable{
    private String writePath;
    private String schema1;
    private String uri;
    private Long rule;
    private int num;

    /**
     *
     * @param writePath  例如 /user
     * @param schema1      parquet文件约束
     * @param uri       例如: hdfs:192.168.2.51:8080
     * @param rule       规则，字节数，小于多少字节的文件将会被合并
     */
    public MergeFiles(String writePath,String schema1,String uri,Long rule,int num) {
        this.schema1 = schema1;
        this.writePath = writePath;
        this.uri = uri;
        this.rule = rule;
        this.num = num;
    }

    @Override
    public void run() {
        try {
            Schema schema = new Schema.Parser().parse(schema1);
            //  连接HDFS文件系统
            FileSystem fs = FileSystem.get(new URI(uri), new Configuration());
            //  读取需要合成的文件夹目录里的文件
            FileStatus[] fileStatuses = fs.listStatus(new Path(writePath));
            List<Path> list = new ArrayList<>();
            for (FileStatus one : fileStatuses) {
                //  如果小于2KB，则列入合并名单里
                if (one.getLen() < rule) {
                    list.add(one.getPath());
                }
            }
            if (list.size() > num) {
                Long time = System.currentTimeMillis();
                ParquetWriter<Object> writer = AvroParquetWriter.builder(new Path(writePath+"/"+time+".parquet"))
                        .withSchema(schema)
                        .withCompressionCodec(CompressionCodecName.SNAPPY)
                        .withPageSize(1048576)
                        .withRowGroupSize(128 * 1024 * 1024)
                        .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                        .build();
                GenericRecord gr = new GenericData.Record(schema);
                for (Path path:list){
                    AvroParquetReader<GenericRecord> reader = new AvroParquetReader<GenericRecord>(path);
                    GenericRecord genericRecord;
                    //  得按约束重写，比较麻烦
                    while ((genericRecord=reader.read())!=null){
                        gr.put("name",genericRecord.get("name"));
                        gr.put("id",genericRecord.get("id"));
                        writer.write(gr);
                    }
                    reader.close();
                    fs.delete(path);

                }
                writer.close();


            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
}
