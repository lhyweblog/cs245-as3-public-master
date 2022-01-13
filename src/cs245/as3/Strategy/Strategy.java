package cs245.as3.Strategy;

import cs245.as3.TransactionManager;
import cs245.as3.interfaces.LogManager;
import cs245.as3.interfaces.StorageManager;

import java.util.HashMap;

/**
 * @Author: lhy
 * @Description:
 * @Date: 2022/01/08/19:39
 */
public abstract class Strategy {

    protected LogManager logManager;

    public Strategy(LogManager lm){
        logManager  = lm;
    }

    public abstract void prepare(long txID);

    public abstract void commit(long txID);

    public abstract void writeRedoLog(TransactionManager.WritesetEntry x,long txID);

    public abstract void Recover(StorageManager sm, LogManager lm, HashMap<Long, StorageManager.TaggedValue> latestValues);

    public abstract void updateOffsets(long persisted_tag);
}
