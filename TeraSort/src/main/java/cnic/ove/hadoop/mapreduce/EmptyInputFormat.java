package cnic.ove.hadoop.mapreduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 空输入
 */
public class EmptyInputFormat extends InputFormat<NullWritable,NullWritable> {
    //可以指定，默认为1
    public static Integer split_size = 1;
    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
        List<InputSplit> result = new ArrayList<InputSplit>();
        for(int i = 0; i < split_size; i++) {
            result.add(new EmptyInputSplit());
        }
        return result;
    }
    @Override
    public RecordReader<NullWritable, NullWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new EmptyRecordReader();
    }
}
