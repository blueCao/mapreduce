package cnic.ove.hadoop.mapreduce;

import cnic.ove.hadoop.mapreduce.EmptyInputSplit;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 *  不读取任何数据，只记录任务的长度
 */
public class EmptyRecordReader extends RecordReader<NullWritable,NullWritable> {
    private Long current;
    private Long lenght;
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        lenght = inputSplit.getLength();
        current = new Long(0L);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        current++;
        return ( lenght > current ) ? true : false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public NullWritable getCurrentValue() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return current / (lenght + 0f);
    }

    @Override
    public void close() throws IOException {
    }
}
