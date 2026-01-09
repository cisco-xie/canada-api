package com.example.demo.config;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;

@Slf4j
public class PriorityTaskExecutor {
    // 定义优先级的任务
    public static class PriorityTask implements Runnable, Comparable<PriorityTask> {
        private final Runnable task;
        private final int priority; // 优先级，数字越小优先级越高

        public PriorityTask(Runnable task, int priority) {
            this.task = task;
            this.priority = priority;
        }

        @Override
        public void run() {
            task.run();
        }

        @Override
        public int compareTo(PriorityTask o) {
            // 按优先级进行排序
            return Integer.compare(this.priority, o.priority);
        }
    }

    // 任务包装类，用来包装 CompletableFuture 和优先级
    public static class CompletableFutureTask<T> implements Runnable, Comparable<CompletableFutureTask<T>> {
        private final CompletableFuture<T> completableFuture;
        private final int priority;

        public CompletableFutureTask(CompletableFuture<T> completableFuture, int priority) {
            this.completableFuture = completableFuture;
            this.priority = priority;
        }

        @Override
        public void run() {
            completableFuture.join();  // 等待CompletableFuture执行完成
        }

        @Override
        public int compareTo(CompletableFutureTask<T> o) {
            // 按优先级进行排序
            return Integer.compare(this.priority, o.priority);
        }

        public CompletableFuture<T> getCompletableFuture() {
            return completableFuture;
        }
    }

    /**
     * 优化线程池关闭逻辑
     * @param executorService
     */
    public static void shutdownExecutor(ExecutorService executorService) {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                log.error("任务超时，强制关闭线程池");
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    // 使用优先级队列的线程池
    public static ExecutorService createPriorityThreadPool(int poolSize) {
        // 创建带优先级队列的线程池
        BlockingQueue<Runnable> queue = new PriorityBlockingQueue<>();
        return new ThreadPoolExecutor(
                poolSize, poolSize, 60L, TimeUnit.SECONDS, queue, new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public static void main(String[] args) {
        ExecutorService executorService = createPriorityThreadPool(10);

        // 创建高优先级的任务
        Runnable task1 = () -> {
            System.out.println("Task 1 - High Priority");
        };

        // 创建低优先级的任务
        Runnable task2 = () -> {
            System.out.println("Task 2 - Low Priority");
        };

        // 提交任务到线程池
        executorService.submit(new PriorityTask(task1, 1)); // 高优先级任务
        executorService.submit(new PriorityTask(task2, 2)); // 低优先级任务
    }
}

