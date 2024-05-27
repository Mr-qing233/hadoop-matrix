package org.example.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MatrixReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<Integer, Integer> mapA = new HashMap<>();
        Map<Integer, Integer> mapB = new HashMap<>();

        for (Text value : values) {
            String[] tokens = value.toString().split(",");
            String matrixName = tokens[0];
            int index = Integer.parseInt(tokens[1]);
            int val = Integer.parseInt(tokens[2]);

            if (matrixName.equals("A")) {
                mapA.put(index, val);
            } else if (matrixName.equals("B")) {
                mapB.put(index, val);
            }
        }

        int sum = 0;
        for (int i : mapA.keySet()) {
            if (mapB.containsKey(i)) {
                sum += mapA.get(i) * mapB.get(i);
            }
        }

        context.write(key, new Text(String.valueOf(sum)));
    }
}



