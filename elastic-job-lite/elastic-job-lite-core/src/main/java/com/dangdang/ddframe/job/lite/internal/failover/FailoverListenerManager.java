/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package com.dangdang.ddframe.job.lite.internal.failover;

import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.lite.internal.config.ConfigurationNode;
import com.dangdang.ddframe.job.lite.internal.config.ConfigurationService;
import com.dangdang.ddframe.job.lite.internal.config.LiteJobConfigurationGsonFactory;
import com.dangdang.ddframe.job.lite.internal.instance.InstanceNode;
import com.dangdang.ddframe.job.lite.internal.listener.AbstractJobListener;
import com.dangdang.ddframe.job.lite.internal.listener.AbstractListenerManager;
import com.dangdang.ddframe.job.lite.internal.schedule.JobRegistry;
import com.dangdang.ddframe.job.lite.internal.sharding.ShardingService;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent.Type;

import java.util.List;

/**
 * 失效转移监听管理器.
 *
 * @author zhangliang
 */
public final class FailoverListenerManager extends AbstractListenerManager {

    private final String jobName;

    private final ConfigurationService configService;

    private final ShardingService shardingService;

    private final FailoverService failoverService;

    private final ConfigurationNode configNode;

    private final InstanceNode instanceNode;

    public FailoverListenerManager(final CoordinatorRegistryCenter regCenter, final String jobName) {
        super(regCenter, jobName);
        this.jobName = jobName;
        configService = new ConfigurationService(regCenter, jobName);
        shardingService = new ShardingService(regCenter, jobName);
        failoverService = new FailoverService(regCenter, jobName);
        configNode = new ConfigurationNode(jobName);
        instanceNode = new InstanceNode(jobName);
    }

    @Override
    public void start() {
        addDataListener(new JobCrashedJobListener());
        addDataListener(new FailoverSettingsChangedJobListener());
    }

    private boolean isFailoverEnabled() {
        LiteJobConfiguration jobConfig = configService.load(true);
        return null != jobConfig && jobConfig.isFailover();
    }

    class JobCrashedJobListener extends AbstractJobListener {

        @Override
        protected void dataChanged(final String path, final Type eventType, final String data) {
            //判断: 开启了 Failover = true && 有节点下线的事件
            if (isFailoverEnabled() && Type.NODE_REMOVED == eventType && instanceNode.isInstancePath(path)) {
                //获取下线的节点 instanceId , 比如:
                String jobInstanceId = path.substring(instanceNode.getInstanceFullPath().length() + 1);
                //这个instance不是本实例
                if (jobInstanceId.equals(JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId())) {
                    return;
                }
                //背景: 在zk /${JOB_NAME}/sharding/${ITEM_ID}/failover 节点的value中会存需要执行这个失效转移item的 jobInstanceId
                //获得关闭作业节点( ${JOB_INSTANCE_ID} )对应的 ${JOB_NAME}/sharding/${ITEM_ID}/failover 作业分片项
                //问题: 为什么要通过"关闭节点"获取failoverItems?
                //答: 执行failover的实例又挂了; 比如 item 1 原本是 A instance 执行, A 挂了, item 1 的 failover 变成 B instance ,
                //但 B instance 又挂了, 那它应该将它获取的 failover item 释放掉,让其他instance 继续执行
                //这里 failoverService.getFailoverItems(jobInstanceId) 就是获取挂了的节点,但还在运行中的 failover items
                //TODO failover 是临时节点, instance 挂了, failover不是也应该删除了吗?
                //TODO 个人认为 failover 会导致一个item在多个 instance上执行, 比如 failover item的 ElasticJob execute在 instance B上已经执行完, 但还没有调用ExecutionService.registerJobCompleted(items) 方法 ,instance B 就挂了, 这样会导致 item在其他节点上再次执行
                List<Integer> failoverItems = failoverService.getFailoverItems(jobInstanceId);
                if (!failoverItems.isEmpty()) {
                    for (int each : failoverItems) {
                        //设置失效的分片项标记 /${JOB_NAME}/leader/failover/items/${ITEM_ID}。该数据节点为永久节点，存储空串( "" )
                        failoverService.setCrashedFailoverFlag(each);
                        //如果需要失效转移, 则执行作业失效转移, 使用分布式锁避免取到同一个item
                        failoverService.failoverIfNecessary();
                    }
                } else {
                    //获得关闭作业节点( ${JOB_INSTANCE_ID} )对应的 /${JOB_NAME}/sharding/${ITEM_ID}/instance 作业分片项。
                    for (int each : shardingService.getShardingItems(jobInstanceId)) {
                        //设置失效的分片项标记 /${JOB_NAME}/leader/failover/items/${ITEM_ID}。该数据节点为永久节点，存储空串( "" )
                        failoverService.setCrashedFailoverFlag(each);
                        failoverService.failoverIfNecessary();
                    }
                }
            }
        }
    }

    class FailoverSettingsChangedJobListener extends AbstractJobListener {

        @Override
        protected void dataChanged(final String path, final Type eventType, final String data) {
            if (configNode.isConfigPath(path) && Type.NODE_UPDATED == eventType && !LiteJobConfigurationGsonFactory.fromJson(data).isFailover()) {
                //动态修改了 job config 的 failover配置为 false ,删除所有 failover节点;
                failoverService.removeFailoverInfo();
            }
        }
    }
}
