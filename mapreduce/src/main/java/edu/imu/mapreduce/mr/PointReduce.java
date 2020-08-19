package edu.imu.mapreduce.mr;


import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Component
@Slf4j
public class PointReduce extends Reducer<Text, IntWritable, Text, IntWritable> {


    class country_score{
        String country;
        int score;

        country_score(String country, int score){
            this.country = country;
            this.score = score;
        }
    }

    List<country_score> l = new ArrayList<>();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }
        l.add(new country_score(key.toString(), count));

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(int i = 0; i < l.size(); i++){
            for(int j = 0; j < l.size() - 1 - i; j++){
                if(l.get(j).score < l.get(j + 1).score) {
                    country_score t = l.get(j);
                    l.set(j, l.get(j + 1));
                    l.set(j + 1, t);
                }
            }
        }
        for (country_score item : l){
            log.info("country:" + item.country + " socre:" + item.score);
            context.write(new Text(item.country + ","), new IntWritable(item.score));
        }
    }
}
