package edu.imu.mapreduce.mr;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@Slf4j
public class MatchWinrateMapper extends Mapper<Object, Text, Text, IntWritable> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        //homecountry 主场球队
        //awaycountry 客场球队
        //home_goal_difference 主场净胜球
        //away_goal_difference 客场净胜球
        // home_result 主场比赛结果
        // away_result 客场比赛结果
        int home_result = 0;
        int away_result = 0;
        String tmp = value.toString();
        String[] spilted = tmp.split(",");

        StringBuffer home_away_country = new StringBuffer(spilted[3].toString());
        home_away_country.append(",");
        home_away_country.append(spilted[6].toString());

        StringBuffer away_home_country = new StringBuffer(spilted[6].toString());
        away_home_country.append(",");
        away_home_country.append(spilted[3].toString());

        int home_goal_difference = Integer.parseInt(spilted[4] ) - Integer.parseInt(spilted[5] );
        int away_goal_difference = Integer.parseInt(spilted[5] ) - Integer.parseInt(spilted[4] );
        if(home_goal_difference > 0)
            home_result = 1;
        if(home_goal_difference == 0)
            home_result = 0;
        if(home_goal_difference < 0)
            home_result = -1;

        if(away_goal_difference > 0)
            away_result = 1;
        if(away_goal_difference == 0)
            away_result = 0;
        if(away_goal_difference < 0)
            away_result = -1;

//        log.info("country:" + home_away_country.toString() + "  res:" + home_result);
        context.write(new Text(home_away_country.toString()),new IntWritable(home_result));
        context.write(new Text(away_home_country.toString()),new IntWritable(away_result));


    }
}
