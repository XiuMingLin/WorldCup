package edu.imu.mapreduce.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    static IntWritable sumWritable = new IntWritable(1);

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        for(IntWritable intWritable : values){
            context.write(sumWritable, key);
            sumWritable = new IntWritable(sumWritable.get() + 1);
        }
    }
}
