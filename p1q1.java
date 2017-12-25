/**
 * Created by zhuzikang on 10/6/17.
 */
import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class p1q1 {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Text comma = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String valStr = value.toString();
            String[] aa = valStr.split(",");
            comma.set(aa[0]);
            if(!Character.isDigit(aa[0].charAt(0))) return;
//            String content = new String();
            for (int i = 1; i < aa.length; i++) {
                if (!aa[i].equals("0")) {
                    aa[i] = "1";
                }
                word.set(aa[i]);
                context.write(comma,word);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (Text val : values) {
                String[] bb = val.toString().split(",");
                for (int l = 0; l < bb.length; l++) {
                    if (bb[l].equals("1")) {
                        sum += 1;
                    }
                }
                System.out.println(bb[0]);
            }
            result.set(sum);
            context.write(key, result);
        }
    }

//
//    private static void deleteFolder(String input) {
//        File index = new File(input);
//        if (!index.exists()) {
//            return;
//        }
//        String[] entries = index.list();
//        for (String s : entries) {
//            File currentFile = new File(index.getPath(), s);
//            currentFile.delete();
//        }
//        index.delete();
//
//    }

    public static void main(String[] args) throws Exception {
//        deleteFolder(args[1]);
        GenericOptionsParser parser = new GenericOptionsParser(new Configuration(), args);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "p1q1");
        job.setJarByClass(p1q1.class);
        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        job.getConfiguration().set("mapreduce.job.queuename", "eecs498f17");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
