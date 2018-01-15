package cnic.ove.hadoop.mapreduce;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 一个不包含任何输入数据的split，用于触发Mapper任务产生数据
 */
public class EmptyInputSplit extends InputSplit implements Writable {

    //默认所有长度都为 1024 * 1024 * 1024 = 1 G 数据
//    public static final Long SPLIT_LENGTH = new Long(1024 * 1024 * 1024L);
    public static final Long SPLIT_LENGTH = new Long(1024 * 1024);

    @Override
    public long getLength() throws IOException, InterruptedException {
        return SPLIT_LENGTH;
    }

    //不提供位置信息
    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }

    public void write(DataOutput dataOutput) throws IOException {
//        WritableUtils.writeVInt(dataOutput,0);
    }

    public void readFields(DataInput dataInput) throws IOException {
//        WritableUtils.readVInt(dataInput);
    }
}
