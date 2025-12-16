package restaurant;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

final class KitchenDispatcher implements Runnable {
    private final BlockingQueue<Order> kitchenQueue;
    private final ExecutorService cooksPool;
    private final Semaphore cookSlots;
    private final EventLogger logger;
    private final StatsCollector stats;

    private final AtomicBoolean stopRequested = new AtomicBoolean(false);

    KitchenDispatcher(BlockingQueue<Order> kitchenQueue,
                      ExecutorService cooksPool,
                      Semaphore cookSlots,
                      EventLogger logger,
                      StatsCollector stats) {
        this.kitchenQueue = kitchenQueue;
        this.cooksPool = cooksPool;
        this.cookSlots = cookSlots;
        this.logger = logger;
        this.stats = stats;
    }

    void requestStop() {
        stopRequested.set(true);
    }

    @Override
    public void run() {
        try {
            while (true) {
                if (stopRequested.get() && kitchenQueue.isEmpty()) {
                    logger.log("КУХНЯ", "-", "диспетчер остановлен (очередь пуста)");
                    break;
                }

                if (!cookSlots.tryAcquire(200, TimeUnit.MILLISECONDS)) {
                    continue;
                }

                Order order = kitchenQueue.poll(200, TimeUnit.MILLISECONDS);
                if (order == null) {
                    cookSlots.release();
                    continue;
                }

                long waitMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - order.enqueuedAtNanos());
                stats.onDequeuedFromKitchenQueue(waitMs);

                logger.log("КУХНЯ", String.valueOf(order.id()),
                        "передал повару (блюдо=%s, ожидание_в_очереди=%dмс, свободные_повара=%d)",
                        order.dish(), waitMs, cookSlots.availablePermits());

                cooksPool.submit(new CookTask(order, cookSlots, logger, stats));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.log("КУХНЯ", "-", "диспетчер прерван -> остановка");
        } catch (Exception e) {
            logger.log("КУХНЯ", "-", "ошибка диспетчера: %s", e.toString());
        }
    }
}

final class CookTask implements Runnable {
    private final Order order;
    private final Semaphore cookSlots;
    private final EventLogger logger;
    private final StatsCollector stats;

    CookTask(Order order, Semaphore cookSlots, EventLogger logger, StatsCollector stats) {
        this.order = order;
        this.cookSlots = cookSlots;
        this.logger = logger;
        this.stats = stats;
    }

    @Override
    public void run() {
        String cookName = Thread.currentThread().getName();
        long startNanos = System.nanoTime();

        try {
            logger.log("ПОВАР", String.valueOf(order.id()),
                    "начал готовить (%s) для клиента#%d",
                    order.dish(), order.clientId());

            Thread.sleep(order.dish().prepTimeMs());

            Meal meal = new Meal(order.id(), order.clientId(), order.dish(), cookName);
            order.readyFuture().complete(meal);

            long cookMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            stats.onCookFinished(cookName, cookMs);

            logger.log("ПОВАР", String.valueOf(order.id()),
                    "готово (%s, время=%dмс)",
                    order.dish(), cookMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            order.readyFuture().completeExceptionally(e);
            logger.log("ПОВАР", String.valueOf(order.id()), "прерван");
        } catch (Exception e) {
            order.readyFuture().completeExceptionally(e);
            logger.log("ПОВАР", String.valueOf(order.id()), "ошибка: %s", e.toString());
        } finally {
            cookSlots.release();
        }
    }
}

final class NamedThreadFactory implements ThreadFactory {
    private final String baseName;
    private final AtomicInteger seq = new AtomicInteger(1);
    NamedThreadFactory(String baseName) {
        this.baseName = baseName;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setName(baseName + "_" + seq.getAndIncrement());
        t.setDaemon(false);
        return t;
    }
}
