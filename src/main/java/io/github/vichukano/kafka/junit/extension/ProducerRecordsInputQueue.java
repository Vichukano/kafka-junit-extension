package io.github.vichukano.kafka.junit.extension;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class ProducerRecordsInputQueue implements BlockingQueue<ProducerRecord<Object, Object>> {
    private final BlockingQueue<ProducerRecord<Object, Object>> origin;
    private final Producer<Object, Object> producer;

    public ProducerRecordsInputQueue(BlockingQueue<ProducerRecord<Object, Object>> origin,
                                     Producer<Object, Object> producer) {
        this.origin = origin;
        this.producer = producer;
    }

    @Override
    public boolean add(ProducerRecord<Object, Object> record) {
        return origin.add(record);
    }

    @Override
    public boolean offer(ProducerRecord<Object, Object> record) {
        return origin.offer(record);
    }

    @Override
    public ProducerRecord<Object, Object> remove() {
        return origin.remove();
    }

    @Override
    public ProducerRecord<Object, Object> poll() {
        return origin.poll();
    }

    @Override
    public ProducerRecord<Object, Object> element() {
        return origin.element();
    }

    @Override
    public ProducerRecord<Object, Object> peek() {
        return origin.peek();
    }

    @Override
    public void put(ProducerRecord<Object, Object> record) throws InterruptedException {
        origin.put(record);
    }

    @Override
    public boolean offer(ProducerRecord<Object, Object> record, long timeout, TimeUnit unit) throws InterruptedException {
        try {
            producer.send(record).get(timeout, unit);
            producer.flush();
        } catch (Exception e) {
            throw new KafkaQueuesException("Failed to send record: " + record, e);
        }
        return origin.offer(record, timeout, unit);
    }

    @Override
    public ProducerRecord<Object, Object> take() throws InterruptedException {
        return origin.take();
    }

    @Override
    public ProducerRecord<Object, Object> poll(long timeout, TimeUnit unit) throws InterruptedException {
        return origin.poll(timeout, unit);
    }

    @Override
    public int remainingCapacity() {
        return origin.remainingCapacity();
    }

    @Override
    public boolean remove(Object o) {
        return origin.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return origin.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends ProducerRecord<Object, Object>> c) {
        return origin.containsAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return origin.removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return origin.retainAll(c);
    }

    @Override
    public void clear() {
        origin.clear();
    }

    @Override
    public int size() {
        return origin.size();
    }

    @Override
    public boolean isEmpty() {
        return origin.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return origin.contains(o);
    }

    @Override
    public Iterator<ProducerRecord<Object, Object>> iterator() {
        return origin.iterator();
    }

    @Override
    public Object[] toArray() {
        return origin.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return origin.toArray(a);
    }

    @Override
    public int drainTo(Collection<? super ProducerRecord<Object, Object>> c) {
        return origin.drainTo(c);
    }

    @Override
    public int drainTo(Collection<? super ProducerRecord<Object, Object>> c, int maxElements) {
        return origin.drainTo(c, maxElements);
    }
}
