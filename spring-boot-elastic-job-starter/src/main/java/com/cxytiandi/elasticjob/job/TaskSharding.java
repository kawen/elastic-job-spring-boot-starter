package com.cxytiandi.elasticjob.job;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextRefreshedEvent;

import com.dangdang.ddframe.job.api.ShardingContext;
import com.dangdang.ddframe.job.reg.zookeeper.ZookeeperRegistryCenter;

/**
 * 任务分片
 * 只有任务分片需要注入：把多个任务分配到多个服务器上执行，资源均衡利用
 * @author jiang
 */
public class TaskSharding implements ApplicationListener<ContextRefreshedEvent> {
	
	/**
	 * logger
	 */
	private static final Logger logger = LoggerFactory.getLogger(TaskSharding.class);
	
	/**
	 * 注册中心
	 */
	private ZookeeperRegistryCenter registryCenter;
	
	/**
	 * job列表
	 */
	private List<String> jobList;
	
	public TaskSharding(ZookeeperRegistryCenter registryCenter,List<String> jobList) {
		this.registryCenter = registryCenter;	
		this.jobList = jobList;
	}

	/**
	 * 当spring容器初始化完成后执行该方法
	 */
	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		// 项目加载完成后，从注册中心获取所有的任务列表
		jobList = registryCenter.getChildrenKeys("/");
	}

	public ZookeeperRegistryCenter getRegistryCenter() {
		return registryCenter;
	}

	public void setRegistryCenter(ZookeeperRegistryCenter registryCenter) {
		this.registryCenter = registryCenter;
	}
	/**
	 * 任务是否允许执行
	 * 
	 * @param shardingContext
	 * @return
	 */
	public boolean allowRun(ShardingContext shardingContext) {
		if (null == jobList || jobList.isEmpty()) {
			logger.error("没有任务可执行");
			return false;
		}

		// 分片总数
		int shardingTotalCount = shardingContext.getShardingTotalCount();
		if (shardingTotalCount == 1) {
			// 分片数为1的时候直接返回成功（没有分片）
			return true;
		}
		
		// 任务名称
		String jobName = shardingContext.getJobName();
		// 获取该任务在任务列表中的索引位置
		int index = jobList.indexOf(jobName);
		// 任务索引位置对分片总数取余得到分片号
		int shardNo = index % shardingTotalCount; 
		
		// 分配于本作业实例的分片项
		int shardingItem = shardingContext.getShardingItem();
				
		// 判断该分片号是否等于本作业实例的分片项
		if (shardNo == shardingItem) {
			return true;
		}
		return false;
	}

}
