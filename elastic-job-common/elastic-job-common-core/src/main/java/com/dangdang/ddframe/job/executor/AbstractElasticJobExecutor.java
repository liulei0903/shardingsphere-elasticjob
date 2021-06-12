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

package com.dangdang.ddframe.job.executor;

import com.dangdang.ddframe.job.api.ShardingContext;
import com.dangdang.ddframe.job.config.JobRootConfiguration;
import com.dangdang.ddframe.job.event.type.JobExecutionEvent;
import com.dangdang.ddframe.job.event.type.JobStatusTraceEvent.State;
import com.dangdang.ddframe.job.exception.ExceptionUtil;
import com.dangdang.ddframe.job.exception.JobExecutionEnvironmentException;
import com.dangdang.ddframe.job.exception.JobSystemException;
import com.dangdang.ddframe.job.executor.handler.ExecutorServiceHandler;
import com.dangdang.ddframe.job.executor.handler.ExecutorServiceHandlerRegistry;
import com.dangdang.ddframe.job.executor.handler.JobExceptionHandler;
import com.dangdang.ddframe.job.executor.handler.JobProperties;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/**
 * 弹性化分布式作业执行器.
 *
 * @author zhangliang
 */
@Slf4j
public abstract class AbstractElasticJobExecutor {

    @Getter(AccessLevel.PROTECTED)
    private final JobFacade jobFacade;

    @Getter(AccessLevel.PROTECTED)
    private final JobRootConfiguration jobRootConfig;

    private final String jobName;

    private final ExecutorService executorService;

    private final JobExceptionHandler jobExceptionHandler;

    private final Map<Integer, String> itemErrorMessages;

    protected AbstractElasticJobExecutor(final JobFacade jobFacade) {
        this.jobFacade = jobFacade;
        //
        jobRootConfig = jobFacade.loadJobRootConfiguration(true);
        jobName = jobRootConfig.getTypeConfig().getCoreConfig().getJobName();
        //获取线程池service,在已给实例分配到多个item分片时使用线程池并行处理
        executorService = ExecutorServiceHandlerRegistry.getExecutorServiceHandler(jobName, (ExecutorServiceHandler) getHandler(JobProperties.JobPropertiesEnum.EXECUTOR_SERVICE_HANDLER));
        //自定义异常处理器,作用类似 ExecutorService中rejectHandler
        jobExceptionHandler = (JobExceptionHandler) getHandler(JobProperties.JobPropertiesEnum.JOB_EXCEPTION_HANDLER);
        //单个分片的异常错误信息
        itemErrorMessages = new ConcurrentHashMap<>(jobRootConfig.getTypeConfig().getCoreConfig().getShardingTotalCount(), 1);
    }

    private Object getHandler(final JobProperties.JobPropertiesEnum jobPropertiesEnum) {
        String handlerClassName = jobRootConfig.getTypeConfig().getCoreConfig().getJobProperties().get(jobPropertiesEnum);
        try {
            Class<?> handlerClass = Class.forName(handlerClassName);
            if (jobPropertiesEnum.getClassType().isAssignableFrom(handlerClass)) {
                return handlerClass.newInstance();
            }
            return getDefaultHandler(jobPropertiesEnum, handlerClassName);
        } catch (final ReflectiveOperationException ex) {
            return getDefaultHandler(jobPropertiesEnum, handlerClassName);
        }
    }

    private Object getDefaultHandler(final JobProperties.JobPropertiesEnum jobPropertiesEnum, final String handlerClassName) {
        log.warn("Cannot instantiation class '{}', use default '{}' class.", handlerClassName, jobPropertiesEnum.getKey());
        try {
            return Class.forName(jobPropertiesEnum.getDefaultValue()).newInstance();
        } catch (final ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new JobSystemException(e);
        }
    }

    /**
     * 执行作业.
     */
    public final void execute() {
        try {
            // 检查作业执行环境
            // TODO 为什么要求本机时间与zk时间误差是否在1秒钟之内
            jobFacade.checkJobExecutionEnvironment();
        } catch (final JobExecutionEnvironmentException cause) {
            jobExceptionHandler.handleException(jobName, cause);
        }
        //关键逻辑:获取本实例本次要执行的分片 items 信息
        ShardingContexts shardingContexts = jobFacade.getShardingContexts();
        if (shardingContexts.isAllowSendJobEvent()) {
            //发送 TASK_STAGING status evnet
            jobFacade.postJobStatusTraceEvent(shardingContexts.getTaskId(), State.TASK_STAGING, String.format("Job '%s' execute begin.", jobName));
        }
        //判断:如果开启了MonitorExecution=ture,并且 items中有任意一个还在运行中,就在zk中为所有items打上错过执行的misfire标记
        if (jobFacade.misfireIfRunning(shardingContexts.getShardingItemParameters().keySet())) {
            if (shardingContexts.isAllowSendJobEvent()) {
                jobFacade.postJobStatusTraceEvent(shardingContexts.getTaskId(), State.TASK_FINISHED, String.format(
                        "Previous job '%s' - shardingItems '%s' is still running, misfired job will start after previous job completed.", jobName,
                        shardingContexts.getShardingItemParameters().keySet()));
            }
            //注意这里打上misfire标记后直接返回了,同一个item在同一个实例上不会并发执行
            return;
        }
        try {
            //执行 before listener
            jobFacade.beforeJobExecuted(shardingContexts);
            //CHECKSTYLE:OFF
        } catch (final Throwable cause) {
            //CHECKSTYLE:ON
            jobExceptionHandler.handleException(jobName, cause);
        }
        //执行分片
        execute(shardingContexts, JobExecutionEvent.ExecutionSource.NORMAL_TRIGGER);
        //本次任务执行完后,判断是否需要执行错过的任务
        while (jobFacade.isExecuteMisfired(shardingContexts.getShardingItemParameters().keySet())) {
            //先清空 misfire 标记在执行
            jobFacade.clearMisfire(shardingContexts.getShardingItemParameters().keySet());
            //再执行一次错估的任务
            execute(shardingContexts, JobExecutionEvent.ExecutionSource.MISFIRE);
        }
        //在 JobCrashedJobListener 中也会触发  jobFacade.failoverIfNecessary(); 如果说作业分片项实现转移时，每个作业节点都不处于非空闲状态，岂不是 FailoverLeaderExecutionCallback 一直无法被回调？
        //答案当然不是的。作业在执行完分配给自己的作业分片项，会调用 LiteJobFacade#failoverIfNecessary() 方法，进行失效转移的作业分片项抓取：
        jobFacade.failoverIfNecessary();
        try {
            //执行after的listener
            jobFacade.afterJobExecuted(shardingContexts);
            //CHECKSTYLE:OFF
        } catch (final Throwable cause) {
            //CHECKSTYLE:ON
            jobExceptionHandler.handleException(jobName, cause);
        }
    }

    private void execute(final ShardingContexts shardingContexts, final JobExecutionEvent.ExecutionSource executionSource) {
        if (shardingContexts.getShardingItemParameters().isEmpty()) {
            if (shardingContexts.isAllowSendJobEvent()) {
                jobFacade.postJobStatusTraceEvent(shardingContexts.getTaskId(), State.TASK_FINISHED, String.format("Sharding item for job '%s' is empty.", jobName));
            }
            return;
        }
        // 注册作业启动信息 setJobRunning(jobName, true);  如果开启了 monitorExecution , 还会在zk中给 item设置 running 标记
        jobFacade.registerJobBegin(shardingContexts);
        String taskId = shardingContexts.getTaskId();
        if (shardingContexts.isAllowSendJobEvent()) {
            jobFacade.postJobStatusTraceEvent(taskId, State.TASK_RUNNING, "");
        }
        try {
            process(shardingContexts, executionSource);
        } finally {
            // TODO 考虑增加作业失败的状态，并且考虑如何处理作业失败的整体回路
            // 注册作业完成信息 , 与  jobFacade.registerJobBegin(shardingContexts); 相对应
            jobFacade.registerJobCompleted(shardingContexts);
            if (itemErrorMessages.isEmpty()) {
                if (shardingContexts.isAllowSendJobEvent()) {
                    jobFacade.postJobStatusTraceEvent(taskId, State.TASK_FINISHED, "");
                }
            } else {
                if (shardingContexts.isAllowSendJobEvent()) {
                    jobFacade.postJobStatusTraceEvent(taskId, State.TASK_ERROR, itemErrorMessages.toString());
                }
            }
        }
    }

    private void process(final ShardingContexts shardingContexts, final JobExecutionEvent.ExecutionSource executionSource) {
        Collection<Integer> items = shardingContexts.getShardingItemParameters().keySet();
        // 单分片，直接执行
        if (1 == items.size()) {
            int item = shardingContexts.getShardingItemParameters().keySet().iterator().next();
            JobExecutionEvent jobExecutionEvent = new JobExecutionEvent(shardingContexts.getTaskId(), jobName, executionSource, item);
            // 执行一个作业
            process(shardingContexts, item, jobExecutionEvent);
            return;
        }
        //通过线程池执行让每个线程执行一个item分片
        final CountDownLatch latch = new CountDownLatch(items.size());
        for (final int each : items) {
            final JobExecutionEvent jobExecutionEvent = new JobExecutionEvent(shardingContexts.getTaskId(), jobName, executionSource, each);
            if (executorService.isShutdown()) {
                return;
            }
            executorService.submit(new Runnable() {

                @Override
                public void run() {
                    try {
                        process(shardingContexts, each, jobExecutionEvent);
                    } finally {
                        latch.countDown();
                    }
                }
            });
        }
        try {
            latch.await();
        } catch (final InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private void process(final ShardingContexts shardingContexts, final int item, final JobExecutionEvent startEvent) {
        if (shardingContexts.isAllowSendJobEvent()) {
            jobFacade.postJobExecutionEvent(startEvent);
        }
        log.trace("Job '{}' executing, item is: '{}'.", jobName, item);
        JobExecutionEvent completeEvent;
        try {
            process(new ShardingContext(shardingContexts, item));
            completeEvent = startEvent.executionSuccess();
            log.trace("Job '{}' executed, item is: '{}'.", jobName, item);
            if (shardingContexts.isAllowSendJobEvent()) {
                jobFacade.postJobExecutionEvent(completeEvent);
            }
            // CHECKSTYLE:OFF
        } catch (final Throwable cause) {
            // CHECKSTYLE:ON
            completeEvent = startEvent.executionFailure(cause);
            jobFacade.postJobExecutionEvent(completeEvent);
            itemErrorMessages.put(item, ExceptionUtil.transform(cause));
            jobExceptionHandler.handleException(jobName, cause);
        }
    }

    protected abstract void process(ShardingContext shardingContext);
}
