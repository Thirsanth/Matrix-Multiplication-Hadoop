package matrix_mul_new;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatMul {

    public static class MatMulMapper extends Mapper<Object, Text, Text, Text> {
        private int m, n, p;

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            m = conf.getInt("m", 2);
            n = conf.getInt("n", 2);
            p = conf.getInt("p", 2);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            String matrixName = parts[0];
            int row = Integer.parseInt(parts[1]);
            int col = Integer.parseInt(parts[2]);
            int val = Integer.parseInt(parts[3]);

            if (matrixName.equals("A")) {
                for (int k = 0; k < p; k++) {
                    context.write(new Text(row + "," + k), new Text("A," + col + "," + val));
                }
            } else if (matrixName.equals("B")) {
                for (int i = 0; i < m; i++) {
                    context.write(new Text(i + "," + col), new Text("B," + row + "," + val));
                }
            }
        }
    }

    public static class MatMulReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int[] a = new int[100];
            int[] b = new int[100];

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if (parts[0].equals("A")) {
                    a[Integer.parseInt(parts[1])] = Integer.parseInt(parts[2]);
                } else {
                    b[Integer.parseInt(parts[1])] = Integer.parseInt(parts[2]);
                }
            }

            int result = 0;
            for (int i = 0; i < 100; i++) {
                result += a[i] * b[i];
            }
            context.write(key, new Text(Integer.toString(result)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("m", 2); // number of rows of A
        conf.setInt("n", 2); // number of columns of A / rows of B
        conf.setInt("p", 2); // number of columns of B

        Job job = Job.getInstance(conf, "Matrix Multiplication");
        job.setJarByClass(MatMul.class);
        job.setMapperClass(MatMulMapper.class);
        job.setReducerClass(MatMulReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
