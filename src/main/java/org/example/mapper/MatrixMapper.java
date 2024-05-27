package org.example.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MatrixMapper extends Mapper<Object, Text, Text, Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        if (tokens.length == 4) {
            String matrixName = tokens[0];
            String row = tokens[1];
            String col = tokens[2];
            String val = tokens[3];

            if (matrixName.equals("A")) {
                for (int k = 0; k < 2; k++) { // B的列数
                    outKey.set(row + "," + k);
                    outValue.set("A," + col + "," + val);
                    context.write(outKey, outValue);
                }
            } else if (matrixName.equals("B")) {
                for (int i = 0; i < 2; i++) { // A的行数
                    outKey.set(i + "," + col);
                    outValue.set("B," + row + "," + val);
                    context.write(outKey, outValue);
                }
            }
        }
    }
}



