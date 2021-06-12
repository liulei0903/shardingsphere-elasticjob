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

package com.dangdang.ddframe.job.lite.internal.schedule;

import com.dangdang.ddframe.job.config.dataflow.DataflowJobConfiguration;
import com.dangdang.ddframe.job.context.TaskContext;
import com.dangdang.ddframe.job.event.JobEventBus;
import com.dangdang.ddframe.job.event.type.JobExecutionEvent;
import com.dangdang.ddframe.job.event.type.JobStatusTraceEvent;
import com.dangdang.ddframe.job.event.type.JobStatusTraceEvent.Source;
import com.dangdang.ddframe.job.event.type.JobStatusTraceEvent.State;
import com.dangdang.ddframe.job.exception.JobExecutionEnvironmentException;
import com.dangdang.ddframe.job.executor.JobFacade;
import com.dangdang.ddframe.job.executor.ShardingContexts;
import com.dangdang.ddframe.job.lite.api.listener.ElasticJobListener;
import com.dangdang.ddframe.job.lite.config.LiteJobConfiguration;
import com.dangdang.ddframe.job.lite.internal.config.ConfigurationService;
import com.dangdang.ddframe.job.lite.internal.sharding.ExecutionContextService;
import com.dangdang.ddframe.job.lite.internal.sharding.ExecutionService;
import com.dangdang.ddframe.job.lite.internal.failover.FailoverService;
import com.dangdang.ddframe.job.lite.internal.sharding.ShardingService;
import com.dangdang.ddframe.job.reg.base.CoordinatorRegistryCenter;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.List;

/**
 * 为作业提供内部服务的门面类.
 *
 * @author zhangliang
 */
@Slf4j
public final class LiteJobFacade implements JobFacade {

    private final ConfigurationService configService;

    private final ShardingService shardingService;

    private final ExecutionContextService executionContextService;

    private final ExecutionService executionService;

    private final FailoverService failoverService;

    private final List<ElasticJobListener> elasticJobListeners;

    private final JobEventBus jobEventBus;

    public LiteJobFacade(final CoordinatorRegistryCenter regCenter, final String jobName, final List<ElasticJobListener> elasticJobListeners, final JobEventBus jobEventBus) {
        configService = new ConfigurationService(regCenter, jobName);
        shardingService = new ShardingService(regCenter, jobName);
        executionContextService = new ExecutionContextService(regCenter, jobName);
        executionService = new ExecutionService(regCenter, jobName);
        failoverService = new FailoverService(regCenter, jobName);
        this.elasticJobListeners = elasticJobListeners;
        this.jobEventBus = jobEventBus;
    }

    @Override
    public LiteJobConfiguration loadJobRootConfiguration(final boolean fromCache) {
        return configService.load(fromCache);
    }

    @Override
    public void checkJobExecutionEnvironment() throws JobExecutionEnvironmentException {
        configService.checkMaxTimeDiffSecondsTolerable();
    }

    @Override
    public void failoverIfNecessary() {
        //config 配置了 failover=true
        if (configService.load(true).isFailover()) {
            failoverService.failoverIfNecessary();
        }
    }

    /**
     * PS:这里的作业启动指在执行 execute的作业,在执行execute之前 JobRegistry.getInstance().setJobRunning(jobName, true);
     *
     * @param shardingContexts 分片上下文
     */
    @Override
    public void registerJobBegin(final ShardingContexts shardingContexts) {
        executionService.registerJobBegin(shardingContexts);
    }

    /**
     * PS:这里的作业启动指在执行 execute的作业,在执行execute之后 JobRegistry.getInstance().setJobRunning(jobName, false);
     *
     * @param shardingContexts 分片上下文
     */
    @Override
    public void registerJobCompleted(final ShardingContexts shardingContexts) {
        executionService.registerJobCompleted(shardingContexts);
        if (configService.load(true).isFailover()) {
            //删除 /${JOB_NAME}/sharding/${ITEM_ID}/failover 节点,完成闭环
            failoverService.updateFailoverComplete(shardingContexts.getShardingItemParameters().keySet());
        }
    }

    @Override
    public ShardingContexts getShardingContexts() {
        //判断是否配置了Failover失效转移
        boolean isFailover = configService.load(true).isFailover();
        if (isFailover) {
            //获取运行在本作业服务器的失效转移分片项集合.
            List<Integer> failoverShardingItems = failoverService.getLocalFailoverItems();
            if (!failoverShardingItems.isEmpty()) {
                //根据 failover 获取分片上下文
                //这里只返回要执行failover的分片,那本机应该执行的 item分片不就丢失了吗?
                //答案是不会,因为获取 failover items的前提是本实例非running状态,也就是本实例已经将本职工作 items 执行完了
                return executionContextService.getJobShardingContext(failoverShardingItems);
            }
        }
        //如果需要的话执行重分片逻辑 , 该方法会先获取分布式锁,未获取到则等待leader分片结束才返回
        shardingService.shardingIfNecessary();
        //获取当前节点的分片
        List<Integer> shardingItems = shardingService.getLocalShardingItems();
        if (isFailover) {
            /**
             *当本作业节点不存在抓取的失效转移分片项，则获得分配给本作业分解的作业分片项。此时你会看到略奇怪的方法调用，
             * shardingItems.removeAll(failoverService.getLocalTakeOffItems())。
             * 为什么呢？举个例子，作业节点A持有作业分片项[0, 1]，此时异常断网，导致[0, 1]被作业节点B失效转移抓取，
             * 此时若作业节点A恢复，作业分片项[0, 1]依然属于作业节点A，但是可能已经在作业节点B执行，因此需要进行移除，
             * 避免多节点运行相同的作业分片项。
             */
            shardingItems.removeAll(failoverService.getLocalTakeOffItems());
        }
        shardingItems.removeAll(executionService.getDisabledItems(shardingItems));
        return executionContextService.getJobShardingContext(shardingItems);
    }

    @Override
    public boolean misfireIfRunning(final Collection<Integer> shardingItems) {
        return executionService.misfireIfHasRunningItems(shardingItems);
    }

    @Override
    public void clearMisfire(final Collection<Integer> shardingItems) {
        executionService.clearMisfire(shardingItems);
    }

    /**
     * 判断是否能够执行 masfired 逻辑
     *
     * @param shardingItems 任务分片项集合
     * @return
     */
    @Override
    public boolean isExecuteMisfired(final Collection<Integer> shardingItems) {
        //1.isEligibleForJobRunning() 先判断是否需要重新分片,返回true标识不需要重分片
        //2.job config是否配置 Misfire = true,默认为 true
        //3.判断 zk的items节点是否存在 misfire 标记
        return isEligibleForJobRunning() && configService.load(true).getTypeConfig().getCoreConfig().isMisfire() && !executionService.getMisfiredJobItems(shardingItems).isEmpty();
    }

    @Override
    public boolean isEligibleForJobRunning() {
        LiteJobConfiguration liteJobConfig = configService.load(true);
        if (liteJobConfig.getTypeConfig() instanceof DataflowJobConfiguration) {
            return !shardingService.isNeedSharding() && ((DataflowJobConfiguration) liteJobConfig.getTypeConfig()).isStreamingProcess();
        }
        return !shardingService.isNeedSharding();
    }

    @Override
    public boolean isNeedSharding() {
        return shardingService.isNeedSharding();
    }

    @Override
    public void beforeJobExecuted(final ShardingContexts shardingContexts) {
        for (ElasticJobListener each : elasticJobListeners) {
            each.beforeJobExecuted(shardingContexts);
        }
    }

    @Override
    public void afterJobExecuted(final ShardingContexts shardingContexts) {
        for (ElasticJobListener each : elasticJobListeners) {
            each.afterJobExecuted(shardingContexts);
        }
    }

    @Override
    public void postJobExecutionEvent(final JobExecutionEvent jobExecutionEvent) {
        jobEventBus.post(jobExecutionEvent);
    }

    @Override
    public void postJobStatusTraceEvent(final String taskId, final State state, final String message) {
        TaskContext taskContext = TaskContext.from(taskId);
        jobEventBus.post(new JobStatusTraceEvent(taskContext.getMetaInfo().getJobName(), taskContext.getId(),
                taskContext.getSlaveId(), Source.LITE_EXECUTOR, taskContext.getType(), taskContext.getMetaInfo().getShardingItems().toString(), state, message));
        if (!Strings.isNullOrEmpty(message)) {
            log.trace(message);
        }
    }
}
