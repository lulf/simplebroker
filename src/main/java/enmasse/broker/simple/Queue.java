package enmasse.broker.simple;

import org.apache.qpid.proton.amqp.messaging.Section;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Queue {
    private final String address;
    private final BlockingQueue<Section> store;

    public Queue(String address, int capacity) {
        this.address = address;
        this.store = new LinkedBlockingQueue<>(capacity);
    }

    public synchronized boolean enqueue(Section body) {
        return store.add(body);
    }

    public synchronized Section dequeue() {
        return store.poll();
    }

    public String getAddress() {
        return this.address;
    }
}
