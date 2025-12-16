package restaurant;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

final class EventLogger {
    private static final String POISON = "__LOGGER_STOP__";
    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private final BlockingQueue<String> q = new LinkedBlockingQueue<>();
    private final Thread thread;

    EventLogger() {
        this.thread = new Thread(this::loop, "Логгер");
        this.thread.setDaemon(true);
    }

    void start() {
        thread.start();
    }

    void log(String role, String orderId, String format, Object... args) {
        String time = LocalTime.now().format(FMT);
        String tname = Thread.currentThread().getName();
        String msg = String.format(format, args);

        String line = String.format(
                "%s | %-18s | %-10s | %6s | %s",
                time,
                trimRight(tname, 18),
                trimRight(role, 10),
                orderId,
                msg
        );
        q.offer(line);
    }

    void close() throws InterruptedException {
        q.offer(POISON);
        thread.join();
    }

    private void loop() {
        try {
            System.out.println("Время        | Поток              | Роль       |  Заказ | Сообщение");
            System.out.println("------------+--------------------+-----------+--------+---------------------------------------------");
            while (true) {
                String line = q.take();
                if (POISON.equals(line)) break;
                System.out.println(line);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    private static String trimRight(String s, int max) {
        return (s.length() <= max) ? s : s.substring(0, max);
    }
}

final class StatsCollector {
    private final LongAdder createdOrders = new LongAdder();
    private final LongAdder dequeuedOrders = new LongAdder();
    private final LongAdder completedOrders = new LongAdder();

    private final LongAdder totalQueueWaitMs = new LongAdder();
    private final AtomicLong maxQueueWaitMs = new AtomicLong(0);

    private final LongAdder totalCookMs = new LongAdder();
    private final AtomicLong maxCookMs = new AtomicLong(0);

    private final Map<String, LongAdder> cookedBy = new ConcurrentHashMap<>();

    void onOrderCreated() {
        createdOrders.increment();
    }

    void onDequeuedFromKitchenQueue(long waitMs) {
        dequeuedOrders.increment();
        totalQueueWaitMs.add(waitMs);
        maxQueueWaitMs.accumulateAndGet(waitMs, Math::max);
    }

    void onCookFinished(String cookThreadName, long cookMs) {
        completedOrders.increment();
        totalCookMs.add(cookMs);
        maxCookMs.accumulateAndGet(cookMs, Math::max);
        cookedBy.computeIfAbsent(cookThreadName, k -> new LongAdder()).increment();
    }

    String snapshot() {
        long created = createdOrders.sum();
        long dequeued = dequeuedOrders.sum();
        long done = completedOrders.sum();

        long avgQueue = (dequeued == 0) ? 0 : totalQueueWaitMs.sum() / dequeued;
        long avgCook = (done == 0) ? 0 : totalCookMs.sum() / done;

        return String.format(
                "заказы(создано=%d, на_кухню=%d, готово=%d), очередь(ср=%dмс, макс=%dмс), готовка(ср=%dмс, макс=%dмс), повара=%s",
                created,
                dequeued,
                done,
                avgQueue,
                maxQueueWaitMs.get(),
                avgCook,
                maxCookMs.get(),
                cookedBySummary()
        );
    }

    private String cookedBySummary() {
        if (cookedBy.isEmpty()) return "{}";
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (var e : cookedBy.entrySet()) {
            if (!first) sb.append(", ");
            first = false;
            sb.append(e.getKey()).append("=").append(e.getValue().sum());
        }
        sb.append("}");
        return sb.toString();
    }
}

final class Monitor implements Runnable {
    private final StatsCollector stats;
    private final BlockingQueue<Order> kitchenQueue;
    private final EventLogger logger;
    private final Semaphore cookSlots;
    private final AtomicBoolean running = new AtomicBoolean(true);

    Monitor(StatsCollector stats, BlockingQueue<Order> kitchenQueue, EventLogger logger, Semaphore cookSlots) {
        this.stats = stats;
        this.kitchenQueue = kitchenQueue;
        this.logger = logger;
        this.cookSlots = cookSlots;
    }

    void stop() {
        running.set(false);
    }

    @Override
    public void run() {
        try {
            while (running.get()) {
                logger.log("МОНИТОР", "-", "очередь_кухни=%d/%d, свободные_повара=%d/%d | %s",
                        kitchenQueue.size(), Config.KITCHEN_QUEUE_CAPACITY,
                        cookSlots.availablePermits(), Config.COOKS_COUNT,
                        stats.snapshot());
                Thread.sleep(Config.MONITOR_PERIOD_MS);
            }
            logger.log("МОНИТОР", "-", "остановлен");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.log("МОНИТОР", "-", "прерван -> остановка");
        }
    }
}
