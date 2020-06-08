package com.cxytiandi.elasticjob.job;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;

import com.dangdang.ddframe.job.api.ShardingContext;
import com.dangdang.ddframe.job.reg.zookeeper.ZookeeperRegistryCenter;

/**
 * 任务分片抽象类
 * 把多个任务分配到多个服务器上面运行
 * 
 * @author jiang
 */
public abstract class AbstractShardingTask extends AbstractTask {

	/**
	 * 任务分片
	 */
	@Autowired(required = false)
	public TaskSharding taskSharding;
	@Autowired
	private ZookeeperRegistryCenter zookeeperRegistryCenter;
	
	/**
	 * 执行作业
	 * 
	 * @param shardingContext  作业运行时多片分片上下文
	 */
	@Override
	public void execute(ShardingContext shardingContext) {
		// 任务分片计算，是否允许执行
		List<String> jobList = zookeeperRegistryCenter.getChildrenKeys("/");
		TaskSharding taskSharding = new TaskSharding(zookeeperRegistryCenter,jobList);
		if (taskSharding.allowRun(shardingContext)) {
			super.execute(shardingContext);
		}
	}
	
}
