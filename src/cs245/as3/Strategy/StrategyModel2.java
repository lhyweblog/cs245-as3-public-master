package cs245.as3.Strategy;

import cs245.as3.TransactionManager;
import cs245.as3.interfaces.LogManager;
import cs245.as3.interfaces.StorageManager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: lhy
 * @Description:
 * 事务持久化策略2
 * 使用不定长的 record 进行记录，读取时先读取单条日志记录长度，再读取该日志内容
 * 优点：充分利用日志空间，读取性能强，恢复速度快
 * 缺点：日志内容可读性差，操作复杂
 * @Date: 2022/01/10/19:30
 */
public class StrategyModel2 extends Strategy {

    public StrategyModel2(LogManager lm) {
        super(lm);
    }

    @Override
    public void prepare(long txID) {
        ByteBuffer prepare = ByteBuffer.allocate(14);
        prepare.putInt(10);
        prepare.put((byte)1);
        prepare.put((byte)0);
        prepare.putLong(txID);
        logManager.appendLogRecord(prepare.array());
    }

    @Override
    public void commit(long txID) {
        ByteBuffer commit= ByteBuffer.allocate(14);
        commit.putInt(10);
        commit.put((byte)1);
        commit.put((byte)1);
        commit.putLong(txID);
        logManager.appendLogRecord(commit.array());
    }

    @Override
    public void writeRedoLog(TransactionManager.WritesetEntry x,long txID) {
        int data_len = 10 + x.value.length;
        int buffer_len = data_len + 4;
        ByteBuffer upLog = ByteBuffer.allocate(buffer_len);
        upLog.putInt(data_len);
        upLog.put((byte)0);
        upLog.put((byte)0);
        upLog.putLong(x.key);
        upLog.put(x.value);
        logManager.appendLogRecord(upLog.array());
    }

    @Override
    public void Recover(StorageManager sm, LogManager lm, HashMap<Long, StorageManager.TaggedValue> latestValues) {
        int index = logManager.getLogTruncationOffset()+4;
        List<ByteBuffer> recordList= new ArrayList<>();
        Map<Long,Integer> txID_tag_map = new HashMap<>();

        int len = 10 + 4;

        while(index<lm.getLogEndOffset()){

            if(index + len > lm.getLogEndOffset())
                len = len -4;

            ByteBuffer record = ByteBuffer.allocate(len);
            record.put(lm.readLogRecord(index,len));

            long txID = record.getLong(2);

            if(record.get(0) == 0){
                recordList.add(record);
            }else{
                if(record.get(1) == 0){
                    recordList.clear();
                    txID_tag_map.put(txID,index);
                }else{
                    for(ByteBuffer bbf : recordList){
                        long key =  bbf.getLong(2);
                        bbf.position(10);
                        int data_size = bbf.capacity() - bbf.position()-4;
                        byte[] bytes = new byte[data_size];
                        for(int i=0 ;i <data_size;i++){
                            bytes[i] = bbf.get();
                        }
                        // 恢复持久化
                        sm.queueWrite(key,txID_tag_map.get(txID),bytes);

                        // 恢复事务
                        latestValues.put(key, new StorageManager.TaggedValue(txID_tag_map.get(txID), bytes));
                    }
                }
            }
            index=index + len;
            len = getDataLen(record) + 4;
        }
    }

    @Override
    public void updateOffsets(long persisted_tag) {
        if(persisted_tag <= logManager.getLogTruncationOffset() || persisted_tag >= logManager.getLogEndOffset())
            return;

        int index = (int)persisted_tag + 4;
        int len = 10 + 4;
        ByteBuffer record = ByteBuffer.allocate(len);
        record.put(logManager.readLogRecord(index,len));
        long txID_Prepare = record.getLong(2);
        index=index + len;
        len = getDataLen(record) + 4;

        if(record.get(0)==0)
            return ;

        while(index<logManager.getLogEndOffset()){

            if(index + len > logManager.getLogEndOffset())
                len = len -4;

            record = ByteBuffer.allocate(len);
            record.put(logManager.readLogRecord(index,len));

            if(record.get(0) == 1) {
                if (record.get(1) == 1) {
                    long txID_commit = record.getLong(2);
                    if (txID_commit == txID_Prepare) {
                        logManager.setLogTruncationOffset((int) persisted_tag);
                        return;
                    }
                } else {
                    return;
                }
            }
            index=index + len;
            len = getDataLen(record) + 4;
        }
    }

    private int getDataLen(ByteBuffer byteBuffer){
        return byteBuffer.getInt(byteBuffer.capacity() - 4);
    }
}
