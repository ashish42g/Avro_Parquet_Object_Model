package com.apple.irpt.df.jobs.kvs.dataWrite;

import com.apple.irpt.df.jobs.kvs.dataWrite.mapreduce.ParquetMapper;
import com.apple.irpt.df.jobs.kvs.dataWrite.mapreduce.ParquetReducer;
import com.apple.irpt.df.jobs.kvs.dataWrite.schema.aggrDaily.Avro_aggrDaily_KVSDlyAggr0;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.Log;
import org.apache.parquet.avro.AvroParquetInputFormat;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.Iterator;

public class ParquetDriver extends Configured implements Tool {
    private static final Log LOG = Log.getLog(ParquetDriver.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ParquetDriver(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        if (args.length < 2) {
            LOG.error("Usage: " + getClass().getName() + " INPUTFILE OUTPUTFILE");
            return 1;
        }

        String inputFile = args[0];
        String outputFile = args[1];

        if (inputFile == null) {
            LOG.error("File not found");
            return 1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, getClass().getName());
        job.setJarByClass(ParquetDriver.class);

        job.setInputFormatClass(AvroParquetInputFormat.class);
        AvroParquetInputFormat.addInputPath(job, new Path(inputFile));
        AvroParquetInputFormat.setAvroReadSchema(job, Avro_aggrDaily_KVSDlyAggr0.SCHEMA$);

        //Mapper
        job.setMapperClass(ParquetMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        FileOutputFormat.setOutputPath(job, new Path(outputFile));
        job.setOutputFormatClass(NullOutputFormat.class);

        // Reducer
        job.setReducerClass(ParquetReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.waitForCompletion(true);

        // Execute job and return status
        FileSystem fileSystem1 = FileSystem.get(conf);
        Path outFilePath1 = new Path(outputFile + "/" + "part-r-00000.txt");
        FSDataOutputStream dataOutputStream1 = fileSystem1.create(outFilePath1, true);
        if (outFilePath1 != null) {
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream1));
            Iterator it = job.getCounters().getGroup("COUNT").iterator();

            while (it.hasNext()) {
                Counter c = (Counter) it.next();
                String dim_code = c.getName();

                bufferedWriter.write(dim_code + "\t" + c.getValue());
                bufferedWriter.newLine();
            }

            bufferedWriter.flush();
            bufferedWriter.close();
        }
        return 0;
    }
}
