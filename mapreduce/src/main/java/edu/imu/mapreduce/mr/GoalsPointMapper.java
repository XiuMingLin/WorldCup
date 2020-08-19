package edu.imu.mapreduce.mr;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@Slf4j
public class GoalsPointMapper extends Mapper<Object, Text, Text, IntWritable> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        //homegoals 主场进球
        //awaygoals 课程进球

        String tmp = value.toString();
        String[] spilted = tmp.split(",");

        String homecountry = spilted[3];
        String homegoals = spilted[4];
        String awaygoals = spilted[5];
        String awaycountry = spilted[6];

        context.write(new Text(homecountry),new IntWritable(Integer.parseInt(homegoals)));
        context.write(new Text(awaycountry),new IntWritable(Integer.parseInt(awaygoals)));
    }
}
