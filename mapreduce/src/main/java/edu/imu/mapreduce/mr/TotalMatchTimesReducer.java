package edu.imu.mapreduce.mr;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Component
@Slf4j
public class TotalMatchTimesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    // 比赛类
    class match{
        private String country; // 国家
        private int times;      // 比赛场次

        match(String country, int times){
            this.country = country;
            this.times = times;
        }
    }
    List<match> matchList = new ArrayList<>();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        int count = 0;
        for(IntWritable value : values){
            count += value.get();
        }
        // 向比赛列表里添加比赛
        match tmp = new match(key.toString(), count);
        matchList.add(tmp);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 进行比赛场次的排序并输出
        for(int i = 0; i < matchList.size() - 1; i++){
            for(int j = 0; j < matchList.size() - i - 1; j++){
                if(matchList.get(j).times < matchList.get(j + 1).times){
                    match t = matchList.get(j);
                    matchList.set(j, matchList.get(j + 1));
                    matchList.set(j + 1, t);
                }
            }
        }
        for (int i = 0; i < matchList.size(); i++) {
            log.info(matchList.get(i).country, matchList.get(i).times);
            context.write(new Text(matchList.get(i).country), new IntWritable(matchList.get(i).times));
        }
    }
}
