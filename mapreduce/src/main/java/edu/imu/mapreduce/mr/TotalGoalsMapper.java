package edu.imu.mapreduce.mr;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@Slf4j
public class TotalGoalsMapper  extends Mapper<Object, Text, Text, IntWritable> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        //year 比赛年份
        //homegoals 主场进球
        //awaygoals 课程进球

        String tmp = value.toString();
        String[] spilted = tmp.split(",");

        StringBuffer home_year_country = new StringBuffer(spilted[0].toString());
        home_year_country.append(",");
        home_year_country.append(spilted[3].toString());

        StringBuffer away_year_country = new StringBuffer(spilted[0].toString());
        away_year_country.append(",");
        away_year_country.append(spilted[6].toString());

        String homegoals = spilted[4];
        String awaygoals = spilted[5];

//        log.info(year_country.toString() + "goal:" + (Integer.parseInt(homegoals) + Integer.parseInt(awaygoals)));
        context.write(new Text(home_year_country.toString()),new IntWritable(Integer.parseInt(homegoals)));
        context.write(new Text(away_year_country.toString()),new IntWritable(Integer.parseInt(awaygoals)));
    }
}
