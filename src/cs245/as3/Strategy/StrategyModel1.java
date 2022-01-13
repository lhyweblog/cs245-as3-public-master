package cs245.as3.Strategy;

import cs245.as3.TransactionManager;
import cs245.as3.interfaces.LogManager;
import cs245.as3.interfaces.StorageManager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @Author: lhy
 * @Description:
 * 事务持久化策略1
 * 使用定长记录 record，长数据采用切分的方式进行redo事务持久化
 * 优点：redo log 可读性强，读取操作简单
 * 缺点：对日志内存空间会造成一定程度浪费
 * @Date: 2022/01/08/19:30
 */
public class StrategyModel1 extends Strategy {


    public StrategyModel1(LogManager lm) {
        super(lm);
    }

    @Override
    public void prepare(long txID) {
        // 两阶段提交 prepare 阶段
        ByteBuffer prepare = ByteBuffer.allocate(65);
        prepare.put((byte)1);
        prepare.put((byte)0);
        prepare.putLong(txID);
        logManager.appendLogRecord(prepare.array());
    }

    @Override
    public void commit(long txID) {
        // 两阶段提交 commit 阶段
        ByteBuffer commit = ByteBuffer.allocate(65);
        commit.put((byte)1);
        commit.put((byte)1);
        commit.putLong(txID);
        logManager.appendLogRecord(commit.array());
    }


    @Override
    public void writeRedoLog(TransactionManager.WritesetEntry x,long txID) {
        ByteBuffer upLog = ByteBuffer.allocate(65);
        if(x.value.length < 50){
            upLog.put((byte)0);
            upLog.put((byte)0);
            upLog.putLong(x.key);
            upLog.putInt(x.value.length);
            upLog.put(x.value);
            logManager.appendLogRecord(upLog.array());
        }else{
            upLog.put((byte)0);
            upLog.put((byte)0);
            upLog.putLong(x.key);
            int len1 = x.value.length/2;
            upLog.putInt(len1);
            byte[] bytes = new byte[len1];
            for(int i=0;i<len1;i++){
                bytes[i] = x.value[i];
            }
            upLog.put(bytes);
            logManager.appendLogRecord(upLog.array());
            int len2 = x.value.length - len1;
            bytes = new byte[len2];
            for(int i = len1;i<x.value.length;i++){
                bytes[i-len1] = x.value[i];
            }
            upLog.position(0);
            upLog.put((byte)0);
            upLog.put((byte)1);
            upLog.putLong(x.key);
            upLog.putInt(len2);
            upLog.put(bytes);
            logManager.appendLogRecord(upLog.array());
        }
    }

    @Override
    public void Recover(StorageManager sm, LogManager lm, HashMap<Long, StorageManager.TaggedValue> latestValues) {
        int index = logManager.getLogTruncationOffset();
        List<ByteBuffer> recordList = new ArrayList<>();
        long txID_index = 0;

        while(index < lm.getLogEndOffset()){
            ByteBuffer record = ByteBuffer.allocate(65);
            record.put(lm.readLogRecord(index,65));
            // TODO: record 前两位 00 表示一次性事务操作日志 ，01 表示追加性事务操作日志 ， 10 表示事务开始标志 ， 11 表示事务结束日志
            if(record.get(0) == 0){
                recordList.add(record);
            }else{
                if(record.get(1) == 0){
                    txID_index = index;
                    recordList.clear();
                }else{
                    // TODO: 事务操作日志的格式，0-1：标识符  | 2-9：key  |  10-14：value size  |  15 - value size：value

                    for(ByteBuffer bbf : recordList){
                        int data_size = bbf.getInt(10);
                        long key = bbf.getLong(2);
                        if(bbf.get(0)==0 && bbf.get(1) == 1){
                            bbf.position(14);
                            byte[] bytes = new byte[data_size];
                            for(int i=0 ;i <data_size;i++){
                                bytes[i] = bbf.get();
                            }
                            byte[] fbytes = latestValues.get(key).value;
                            byte[] final_value = new byte[fbytes.length + bytes.length];
                            System.arraycopy(fbytes, 0, final_value, 0, fbytes.length);
                            System.arraycopy(bytes, 0, final_value, fbytes.length, bytes.length);
                            // 恢复持久化
                            sm.queueWrite(key,txID_index,final_value);

                            // 恢复事务
                            latestValues.put(key, new StorageManager.TaggedValue(txID_index, final_value));
                        }else{
                            bbf.position(14);
                            byte[] bytes = new byte[data_size];
                            for(int i=0 ;i <data_size;i++){
                                bytes[i] = bbf.get();
                            }
                            // 恢复持久化
                            sm.queueWrite(key,txID_index,bytes);

                            // 恢复事务
                            latestValues.put(key, new StorageManager.TaggedValue(txID_index, bytes));
                        }
                    }
                }
            }
            index += 65;
        }
    }

    @Override
    public void updateOffsets(long persisted_tag) {

        if(persisted_tag <= logManager.getLogTruncationOffset() || persisted_tag >= logManager.getLogEndOffset())
            return;

        int index = (int)persisted_tag;
        ByteBuffer record = ByteBuffer.allocate(65);
        record.put(logManager.readLogRecord(index,65));
        long txID_prepare = record.getLong(2);
        index+=65;
        while(index < logManager.getLogEndOffset()){
            record = ByteBuffer.allocate(65);
            record.put(logManager.readLogRecord(index,65));
            if(record.get(0) == 1){
                if(record.get(1) == 1){
                    long txID_commit = record.getLong(2);
                    if(txID_commit == txID_prepare){
                        logManager.setLogTruncationOffset((int)persisted_tag);
                        return ;
                    }
                }else {
                    return ;
                }
            }
            index += 65;
        }
    }
}
