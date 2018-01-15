package cnic.ove.hadoop.mapreduce.app;

import cnic.ove.hadoop.mapreduce.EmptyInputFormat;
import cnic.ove.hadoop.mapreduce.NatureComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 利用MapReduce产生一定量的数据，并对这些数据进行排序
 */
public class TeraSort{

    /**
     * 随机数生产者Mapper
     */
    public static class RamdomIntProducerMapper
            extends Mapper<NullWritable, NullWritable, IntWritable, IntWritable> {

        /**
         * 产生随机数，并写入
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(NullWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
            Double ramdom = Integer.MAX_VALUE * Math.random();
            IntWritable random_int = new IntWritable();
            random_int.set(ramdom.intValue());
            context.write(new IntWritable(0),random_int);
        }
    }

    /**
     * 随机数消费者Reducer
     */
    public static class RamdomIntComsumerReducer
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        /**
         * 将数据按照自然顺序，写入一个文件中
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable value : values) {
                context.write(key,value);
            }
        }
    }

    /**
     * 产生数据
     *
     * @param args
     * @throws Exception
     */
    public static void produceData(String[] args) throws Exception {
        if (args.length < 2){
            System.out.println("输入参数少于 2 个");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Terasort generate data");

        //设置jar、mapper、reducer、combiner、outputKey、outputValue
        job.setJarByClass(TeraSort.class);
        job.setMapperClass(RamdomIntProducerMapper.class);
        job.setCombinerClass(RamdomIntComsumerReducer.class);
        job.setReducerClass(RamdomIntComsumerReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        //设置输入格式
        job.setInputFormatClass(EmptyInputFormat.class);

        //设置产生文件的大小,从程序执行的输入参数中获取，单位为G
        long outputFileSize = Integer.valueOf(args[0]);

        //设置数据大小（单位G
        int data_size = Integer.valueOf(args[0]);
        EmptyInputFormat.split_size = data_size;

        //设置输出文件的hdfs文件路径，从程序执行的输入参数中获取
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //设置自然顺序排序
        job.setSortComparatorClass(NatureComparator.class);
        job.setGroupingComparatorClass(NatureComparator.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
            produceData(args);
    }
}

