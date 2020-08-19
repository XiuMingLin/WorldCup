package edu.imu.mapreduce.mr;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class PointMapper extends Mapper<Object, Text, Text, IntWritable> {

    Map<String, Integer> map = new HashMap<>();

    public void readFile(){
        String fileName = "E://mapreduce//mapreduce//res//TeamPoint2.CSV";
        String line = "";
        try {
            BufferedReader in = new BufferedReader(new FileReader(fileName));
            line = in.readLine();
            while(line != null){
                String[] tmp = line.split(",\t");
                map.put(tmp[0], Integer.parseInt(tmp[1]));
                line = in.readLine();
            }
            in.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        readFile();
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        String tmp = value.toString();
        String[] spilted = tmp.split(",");
        String homeCountry = spilted[3];
        String awayCountry = spilted[6];
        int homescore = map.get(homeCountry);
        int awayscore = map.get(awayCountry);
        int point = Integer.parseInt(spilted[4]) - Integer.parseInt(spilted[5]);
        if(point > 0){
            homescore += awayscore / 10;
        }else if(point < 0){
            awayscore += homescore / 10;
        }
        context.write(new Text(homeCountry),new IntWritable(homescore));
        context.write(new Text(awayCountry),new IntWritable(awayscore));
    }
}
