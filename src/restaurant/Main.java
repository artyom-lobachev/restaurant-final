package restaurant;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        Thread.currentThread().setName("Главный");

        EventLogger logger = new EventLogger();
        logger.start();

        StatsCollector stats = new StatsCollector();

        BlockingQueue<Order> kitchenQueue = new LinkedBlockingQueue<>(Config.KITCHEN_QUEUE_CAPACITY);

        ExecutorService cooksPool = Executors.newFixedThreadPool(
                Config.COOKS_COUNT,
                new NamedThreadFactory("Повар")
        );

        // Слоты поваров: диспетчер кухни не возьмёт заказ из очереди, пока нет свободного повара.
        Semaphore cookSlots = new Semaphore(Config.COOKS_COUNT, true);

        KitchenDispatcher dispatcher = new KitchenDispatcher(kitchenQueue, cooksPool, cookSlots, logger, stats);
        Thread dispatcherThread = new Thread(dispatcher, "Диспетчер_Кухни");
        dispatcherThread.start();

        List<Waiter> waiters = new ArrayList<>();
        for (int i = 1; i <= Config.WAITERS_COUNT; i++) {
            Waiter w = new Waiter(i, kitchenQueue, logger);
            waiters.add(w);
            w.start();
        }

        ClientGenerator generator = new ClientGenerator(waiters, logger, stats);
        Thread generatorThread = new Thread(generator, "Генератор_Клиентов");
        generatorThread.start();

        Monitor monitor = new Monitor(stats, kitchenQueue, logger, cookSlots);
        Thread monitorThread = new Thread(monitor, "Монитор");
        monitorThread.start();

        logger.log("СИСТЕМА", "-", "Симуляция запущена: официанты=%d, повара=%d, очередь_кухни=%d",
                Config.WAITERS_COUNT, Config.COOKS_COUNT, Config.KITCHEN_QUEUE_CAPACITY);

        Thread.sleep(Config.SIMULATION_DURATION_MS);

        logger.log("СИСТЕМА", "-", "Останавливаем генератор клиентов...");
        generator.stop();
        generatorThread.join();

        logger.log("СИСТЕМА", "-", "Останавливаем официантов (после обработки своих очередей)...");
        for (Waiter w : waiters) w.stopAccepting();
        for (Waiter w : waiters) w.join();

        logger.log("СИСТЕМА", "-", "Останавливаем диспетчер кухни (когда очередь опустеет)...");
        dispatcher.requestStop();
        dispatcherThread.join();

        logger.log("СИСТЕМА", "-", "Останавливаем пул поваров...");
        cooksPool.shutdown();
        if (!cooksPool.awaitTermination(10, TimeUnit.SECONDS)) {
            logger.log("СИСТЕМА", "-", "Повара не завершились вовремя -> shutdownNow()");
            cooksPool.shutdownNow();
        }

        monitor.stop();
        monitorThread.join();

        logger.log("СИСТЕМА", "-", "Итоговая статистика: %s", stats.snapshot());
        logger.close();
    }
}
