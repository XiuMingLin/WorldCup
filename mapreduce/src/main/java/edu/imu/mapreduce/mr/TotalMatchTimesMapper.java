package edu.imu.mapreduce.mr;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@Slf4j
public class TotalMatchTimesMapper extends Mapper<Object, Text, Text, IntWritable> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        //homecountry 主场球队
        //awaycountry 客场球队
        String tmp = value.toString();
        String[] spilted = tmp.split(",");
        String homecountry = spilted[3];
        String awaycountry = spilted[6];
        context.write(new Text(homecountry), new IntWritable(1));
        context.write(new Text(awaycountry), new IntWritable(1));
    }
}
