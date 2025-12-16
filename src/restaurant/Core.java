package restaurant;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

final class Config {
    private Config() {}

    static final int WAITERS_COUNT = 3;
    static final int COOKS_COUNT = 2;

    static final int KITCHEN_QUEUE_CAPACITY = 10;

    // Клиенты достаточно частые, чтобы при 2 поварах появилась очередь в kitchenQueue.
    static final int CLIENT_MIN_DELAY_MS = 1200;
    static final int CLIENT_MAX_DELAY_MS = 2500;

    static final int WAITER_ACCEPT_DELAY_MS = 500;
    static final int WAITER_DELIVERY_DELAY_MS = 700;

    static final long SIMULATION_DURATION_MS = 30_000;
    static final long MONITOR_PERIOD_MS = 5000;
}

enum DishType {
    САЛАТ(4000),
    СУП(5500),
    ПАСТА(7000),
    СТЕЙК(9000),
    ДЕСЕРТ(6000);

    private final int prepTimeMs;

    DishType(int prepTimeMs) {
        this.prepTimeMs = prepTimeMs;
    }

    int prepTimeMs() {
        return prepTimeMs;
    }

    static DishType randomDish() {
        DishType[] values = values();
        return values[ThreadLocalRandom.current().nextInt(values.length)];
    }
}

final class Meal {
    private final long orderId;
    private final int clientId;
    private final DishType dish;
    private final String cookedBy;

    Meal(long orderId, int clientId, DishType dish, String cookedBy) {
        this.orderId = orderId;
        this.clientId = clientId;
        this.dish = dish;
        this.cookedBy = cookedBy;
    }

    long orderId() { return orderId; }
    int clientId() { return clientId; }
    DishType dish() { return dish; }
    String cookedBy() { return cookedBy; }
}

final class Order {
    private static final AtomicLong ID_GEN = new AtomicLong(1);
    private static final Order POISON = new Order(true);

    private final boolean poison;

    private final long id;
    private final int clientId;
    private final DishType dish;

    private final long createdAtNanos;
    private volatile long enqueuedAtNanos;

    private volatile int waiterId;

    private final CompletableFuture<Meal> readyFuture = new CompletableFuture<>();

    private Order(boolean poison) {
        this.poison = poison;
        this.id = -1;
        this.clientId = -1;
        this.dish = null;
        this.createdAtNanos = System.nanoTime();
    }

    private Order(long id, int clientId, DishType dish) {
        this.poison = false;
        this.id = id;
        this.clientId = clientId;
        this.dish = dish;
        this.createdAtNanos = System.nanoTime();
    }

    static Order create(int clientId, DishType dish) {
        return new Order(ID_GEN.getAndIncrement(), clientId, dish);
    }

    static Order poisonPill() {
        return POISON;
    }

    boolean isPoison() { return poison; }

    long id() { return id; }
    int clientId() { return clientId; }
    DishType dish() { return dish; }

    long createdAtNanos() { return createdAtNanos; }

    void markEnqueued() { this.enqueuedAtNanos = System.nanoTime(); }
    long enqueuedAtNanos() { return enqueuedAtNanos; }

    void setWaiterId(int waiterId) { this.waiterId = waiterId; }
    int waiterId() { return waiterId; }

    CompletableFuture<Meal> readyFuture() { return readyFuture; }
}
