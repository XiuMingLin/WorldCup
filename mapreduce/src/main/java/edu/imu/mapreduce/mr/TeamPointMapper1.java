package edu.imu.mapreduce.mr;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@Slf4j

public class TeamPointMapper1 extends Mapper<Object, Text, Text, IntWritable> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        // Winner 冠军
        // Runners_Up 亚军
        // Third 第三名
        // Fourth 第四名

        String tmp = value.toString();
        String[] spilted = tmp.split(",");
        String Winner = spilted[2];
        String Runners_Up = spilted[3];
        String Third = spilted[4];
        String Fourth = spilted[5];

        context.write(new Text(Winner),new IntWritable(10));
        context.write(new Text(Runners_Up),new IntWritable(5));
        context.write(new Text(Third),new IntWritable(3));
        context.write(new Text(Fourth),new IntWritable(1));




    }
}
