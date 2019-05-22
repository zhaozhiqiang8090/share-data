package com.gomefinance.baitiao.om.util;

import java.security.SecureRandom;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrimaryKeyIdWorker {
	/**
	 * 自定义 ID 生成器
	 * ID 生成规则: ID长达 64 bits
	 * 
	 * | 41 bits: Timestamp (毫秒) | 5 bits: 区域（机房） | 5 bits: 机器编号 | 12 bits: 序列号 |
	 */
	protected static final Logger LOG = LoggerFactory.getLogger(PrimaryKeyIdWorker.class);

	private long workerId;
	private long datacenterId;
	private long sequence = 0L;

	/** 2015-01-01 00:00:00 */
	// public static long twepoch = 1420041600000L;
	public static long twepoch = 1288834974657L;
	// 机器标识位数
	private long workerIdBits = 5L;
	private long datacenterIdBits = 5L;
	// 机器ID最大值
	private long maxWorkerId = -1L ^ (-1L << workerIdBits);
	// 机器ID最大值
	private long maxDatacenterId = -1L ^ (-1L << datacenterIdBits);
	// 序列号识位数
	private long sequenceBits = 12L;

	private long workerIdShift = sequenceBits;
	private long datacenterIdShift = sequenceBits + workerIdBits;
	private long timestampLeftShift = sequenceBits + workerIdBits + datacenterIdBits;
	private long sequenceMask = -1L ^ (-1L << sequenceBits);

	private long lastTimestamp = -1L;

	private Random r = new SecureRandom();//使用强伪随机数提高随机数的安全性

	public PrimaryKeyIdWorker(long workerId, long datacenterId) {
		// sanity check for workerId
		if (workerId > maxWorkerId || workerId < 0) {
			throw new IllegalArgumentException(String.format("worker Id can't be greater than %d or less than 0", maxWorkerId));
		}
		if (datacenterId > maxDatacenterId || datacenterId < 0) {
			throw new IllegalArgumentException(String.format("datacenter Id can't be greater than %d or less than 0", maxDatacenterId));
		}
		this.workerId = workerId;
		this.datacenterId = datacenterId;
		LOG.info(String.format("worker starting. timestamp left shift %d, datacenter id bits %d, worker id bits %d, sequence bits %d, workerid %d", timestampLeftShift, datacenterIdBits, workerIdBits, sequenceBits, workerId));
	}

	public synchronized long nextId() {
		long timestamp = timeGen();

		if (timestamp < lastTimestamp) {
			LOG.error(String.format("clock is moving backwards.  Rejecting requests until %d.", lastTimestamp));
			throw new RuntimeException(String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds", lastTimestamp - timestamp));
		}

		if (lastTimestamp == timestamp) {
			sequence = (sequence + 1) & sequenceMask;
			if (sequence == 0) {
				timestamp = tilNextMillis(lastTimestamp);
			}
		} else {
			// sequence = 0L;
			sequence = r.nextInt(2);
		}

		lastTimestamp = timestamp;

		return ((timestamp - twepoch) << timestampLeftShift) | (datacenterId << datacenterIdShift) | (workerId << workerIdShift) | sequence;
	}

	protected long tilNextMillis(long lastTimestamp) {
		long timestamp = timeGen();
		while (timestamp <= lastTimestamp) {
			timestamp = timeGen();
		}
		return timestamp;
	}

	protected long timeGen() {
		return System.currentTimeMillis();
	}
}