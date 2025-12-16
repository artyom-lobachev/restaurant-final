package restaurant;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

final class Waiter extends Thread {
    private final int waiterId;
    private final BlockingQueue<Order> kitchenQueue;
    private final EventLogger logger;

    private final BlockingQueue<Order> incoming = new LinkedBlockingQueue<>();
    private final BlockingQueue<Meal> readyMeals = new LinkedBlockingQueue<>();

    private boolean accepting = true;
    private int inFlight = 0;

    Waiter(int waiterId, BlockingQueue<Order> kitchenQueue, EventLogger logger) {
        super("Официант_" + waiterId);
        this.waiterId = waiterId;
        this.kitchenQueue = kitchenQueue;
        this.logger = logger;
    }

    void submitOrder(Order order) {
        incoming.offer(order);
    }

    void stopAccepting() {
        incoming.offer(Order.poisonPill());
    }

    @Override
    public void run() {
        try {
            while (accepting || inFlight > 0 || !incoming.isEmpty() || !readyMeals.isEmpty()) {
                // 1) Сначала стараемся выдавать готовые блюда — это имитирует приоритет «донести в зал».
                Meal ready = readyMeals.poll(150, java.util.concurrent.TimeUnit.MILLISECONDS);
                if (ready != null) {
                    Thread.sleep(Config.WAITER_DELIVERY_DELAY_MS);
                    inFlight = Math.max(0, inFlight - 1);
                    logger.log("ОФИЦ", String.valueOf(ready.orderId()),
                            "доставил клиенту#%d (блюдо=%s, готовил=%s)",
                            ready.clientId(), ready.dish(), ready.cookedBy());
                    continue;
                }

                // 2) Затем принимаем новые заказы.
                Order order = incoming.poll(150, java.util.concurrent.TimeUnit.MILLISECONDS);
                if (order == null) {
                    continue;
                }

                if (order.isPoison()) {
                    accepting = false;
                    logger.log("ОФИЦ", "-", "официант#%d больше не принимает новые заказы", waiterId);
                    continue;
                }

                Thread.sleep(Config.WAITER_ACCEPT_DELAY_MS);

                order.setWaiterId(waiterId);
                logger.log("ОФИЦ", String.valueOf(order.id()),
                        "принял заказ от клиента#%d (блюдо=%s)",
                        order.clientId(), order.dish());

                // Готовность блюда «прилетит» обратно официанту в его очередь readyMeals.
                order.readyFuture().thenAccept(readyMeals::offer);

                order.markEnqueued();
                logger.log("ОФИЦ", String.valueOf(order.id()),
                        "отправляю на кухню (очередь кухни сейчас=%d)",
                        kitchenQueue.size());

                kitchenQueue.put(order);
                inFlight++;

                logger.log("ОФИЦ", String.valueOf(order.id()),
                        "заказ поставлен в очередь кухни (размер=%d)",
                        kitchenQueue.size());
            }

            logger.log("ОФИЦ", "-", "официант#%d завершил работу", waiterId);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.log("ОФИЦ", "-", "официант#%d прерван -> остановка", waiterId);
        } catch (Exception e) {
            logger.log("ОФИЦ", "-", "ошибка официанта#%d: %s", waiterId, e.toString());
        }
    }
}

final class ClientGenerator implements Runnable {
    private final List<Waiter> waiters;
    private final EventLogger logger;
    private final StatsCollector stats;
    private final AtomicBoolean running = new AtomicBoolean(true);

    private int nextClientId = 1;

    ClientGenerator(List<Waiter> waiters, EventLogger logger, StatsCollector stats) {
        this.waiters = waiters;
        this.logger = logger;
        this.stats = stats;
    }

    void stop() {
        running.set(false);
    }

    @Override
    public void run() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        try {
            while (running.get()) {
                int clientId = nextClientId++;
                DishType dish = DishType.randomDish();

                Order order = Order.create(clientId, dish);
                stats.onOrderCreated();

                Waiter waiter = waiters.get(rnd.nextInt(waiters.size()));
                logger.log("КЛИЕНТ", String.valueOf(order.id()),
                        "клиент#%d сделал заказ (блюдо=%s) -> %s",
                        clientId, dish, waiter.getName());

                waiter.submitOrder(order);

                Thread.sleep(rnd.nextInt(Config.CLIENT_MIN_DELAY_MS, Config.CLIENT_MAX_DELAY_MS + 1));
            }
            logger.log("КЛИЕНТ", "-", "генератор остановлен");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.log("КЛИЕНТ", "-", "генератор прерван -> остановка");
        }
    }
}
