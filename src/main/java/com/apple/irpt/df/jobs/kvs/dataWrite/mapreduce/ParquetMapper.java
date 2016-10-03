package com.apple.irpt.df.jobs.kvs.dataWrite.mapreduce;

import com.apple.irpt.df.jobs.kvs.dataWrite.schema.aggrDaily.Avro_aggrDaily_KVSDlyAggr0;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class ParquetMapper extends Mapper<LongWritable, Avro_aggrDaily_KVSDlyAggr0, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Avro_aggrDaily_KVSDlyAggr0 value, Context context) throws IOException, InterruptedException {
        if (value != null)
            context.write(new Text(value.getOSVERSION() + "\t" + value.getPRSID()), NullWritable.get());
    }
}
