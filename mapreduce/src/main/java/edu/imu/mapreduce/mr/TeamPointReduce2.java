package edu.imu.mapreduce.mr;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Component
@Slf4j
public class TeamPointReduce2 extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
        int pointcount = 0;
        for(IntWritable value:values)
        {
            pointcount += value.get();
        }
        log.info(key.toString(),pointcount);
        context.write(new Text(key+","),new IntWritable(pointcount));
    }
    }
