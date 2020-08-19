package edu.imu.mapreduce.mr;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@Slf4j

public class TeamPointMapper2 extends Mapper<Object, Text, Text, IntWritable> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        String tmp = value.toString();
        String[] spilted = tmp.split(",\t");
        String country = spilted[0];
        String point = spilted[1];
        context.write(new Text(country),new IntWritable(Integer.parseInt(point)));
    }
}
