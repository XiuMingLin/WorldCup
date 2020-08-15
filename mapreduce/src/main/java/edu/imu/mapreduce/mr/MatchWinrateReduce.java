package edu.imu.mapreduce.mr;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
@Slf4j
public class MatchWinrateReduce extends Reducer<Text, IntWritable, Text, Text> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
        // Total_Match 总比赛数
        // win_Match 获胜比赛
        // Tie_Match 平局
        // lose_Match 失败比赛

        // Win_Rate 胜率
        // Tie_Rate 平局率
        // lose_Rate 失败率

        int Total_Match = 0;
        int win_Match = 0;
        int Tie_Match = 0;
        int lose_Match = 0;

        float Win_Rate = 0;
        float Tie_Rate = 0;
        float lose_Rate = 0;
        for(IntWritable value : values)
        {
            Total_Match += 1;
            if(value.get()==1)
                win_Match += 1;
            if(value.get()==0)
                Tie_Match += 1;
            if(value.get()==-1)
                lose_Match += 1;
        }
        //log.info("country:" + key + " total:" + Total_Match + " win:" + win_Match + " tie:" +Tie_Match + " lose:" + lose_Match);
        Win_Rate = (float) win_Match/(float) Total_Match;
        Tie_Rate = (float) Tie_Match/(float) Total_Match;
        lose_Rate = (float) lose_Match/(float) Total_Match;

        StringBuffer Rate = new StringBuffer();
        Rate.append(Total_Match + "");
        Rate.append(",");
        Rate.append(Win_Rate + "");
        Rate.append(",");
        Rate.append(Tie_Rate + "");
        Rate.append(",");
        Rate.append(lose_Rate + "");

        if(Total_Match>=5)
        context.write(new Text(key),new Text(Rate.toString()));
    }
}
