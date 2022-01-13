package cs245.as3.driver;

import java.nio.ByteBuffer;

import cs245.as3.interfaces.LogManager;

/**
 * DO NOT MODIFY THIS FILE IN THIS PACKAGE **
 * Make this an interface
 */
public class LogManagerImpl implements LogManager {
	private ByteBuffer log;  // 模拟 日志持久化
	private int logSize;     // 日志的长度
	private int logTruncationOffset;  // 日志的截断位置，前面的相当于清楚了，用于redo log 的 数据持久化
	private int Riop_counter;   // 读取的次数
	private int Wiop_counter;   // 写入的次数
	private int nIOSBeforeCrash;    // 设置几次IO之后，log 崩溃   调用 crash 清除数据
	private boolean serveRequests;  // 模拟不可用状态

	// 1 GB
	private static final int BUFSIZE_START = 1000000000;

	// 128 byte max record size
	private static final int RECORD_SIZE = 128;

	public LogManagerImpl() {
		log = ByteBuffer.allocate(BUFSIZE_START);
		logSize = 0;
		serveRequests = true;
	}

	/* **** Public API **** */

	public int getLogEndOffset() {
		return logSize;
	}

	public byte[] readLogRecord(int position, int size) throws ArrayIndexOutOfBoundsException {
		checkServeRequest();
		if ( position < logTruncationOffset || position+size > getLogEndOffset() ) {
			throw new ArrayIndexOutOfBoundsException("Offset " + (position+size) + "invalid: log start offset is " + logTruncationOffset +
					", log end offset is " + getLogEndOffset());
		}

		if ( size > RECORD_SIZE ) {
			throw new IllegalArgumentException("Record length " + size +
					" greater than maximum allowed length " + RECORD_SIZE);
		}

		byte[] ret = new byte[size];
		log.position(position);
		log.get(ret);
		Riop_counter++;
		return ret;
	}

	//Returns the length of the log before the append occurs, atomically
	public int appendLogRecord(byte[] record) {
		checkServeRequest();
		if ( record.length > RECORD_SIZE ) {
			throw new IllegalArgumentException("Record length " + record.length +
					" greater than maximum allowed length " + RECORD_SIZE);
		}
		synchronized(this) {
			Wiop_counter++;

			log.position(logSize);

			for ( int i = 0; i < record.length; i++ ) {
				log.put(record[i]);
			}

			int priorLogSize = logSize;

			logSize += record.length;
			return priorLogSize;
		}
	}

	public int getLogTruncationOffset() {
		return logTruncationOffset;
	}

	public void setLogTruncationOffset(int logTruncationOffset) {
		if (logTruncationOffset > logSize || logTruncationOffset < this.logTruncationOffset) {
			throw new IllegalArgumentException();
		}

		this.logTruncationOffset = logTruncationOffset;
	}


	/* **** For testing only **** */

	protected class CrashException extends RuntimeException {

	}
	/**
	 * We use this to simulate the log becoming inaccessible after a certain number of operations,
	 * for the purposes of simulating crashes.
	 */
	private void checkServeRequest() {
		if (nIOSBeforeCrash > 0) {
			nIOSBeforeCrash--;
			if (nIOSBeforeCrash == 0) {
				serveRequests = false;
			}
		}
		if (!serveRequests) {
			//Crash!
			throw new CrashException();
		}
   		assert(!Thread.interrupted()); //Cooperate with timeout:
	}

	protected int getIOPCount() {
		return Riop_counter + Wiop_counter;
	}

	protected void crash() {
		log.clear();
	}

	protected void stopServingRequestsAfterIOs(int nIOsToServe) {
		nIOSBeforeCrash = nIOsToServe;
	}

	protected void resumeServingRequests() {
		serveRequests = true;
		nIOSBeforeCrash = 0;
	}

}
