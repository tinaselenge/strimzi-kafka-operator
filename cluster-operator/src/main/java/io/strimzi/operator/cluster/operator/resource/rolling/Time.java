/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

/**
 * An abstraction of time. 
 * A Time instance represents some way of measuring durations (the {@link #nanoTime()} method) and some way of suspending 
 * the execution of a thread for some duration consistent with such measurements {@link #sleep(long, int)}.
 * <p>In practice a real system would use {@link #SYSTEM_TIME}.</p>
 * <p>In testing you can use {@link Time.TestTime} so that tests don't depend on actually sleeping threads.</p>
 */
public interface Time {

    public final Time SYSTEM_TIME = new Time() {
        @Override
        public long nanoTime() {
            return System.nanoTime();
        }

        @Override
        public void sleep(long millis, int nanos) throws InterruptedException {
            Thread.sleep(millis, nanos);
        }
    };


    public static class TestTime implements Time {
        long time = 0;

        @Override
        public long nanoTime() {
            return time;
        }

        @Override
        public void sleep(long millis, int nanos) throws InterruptedException {
            time += 1_000_000 * millis + nanos;
        }
    }


    /**
     * The number of nanoseconds since some unknown epoch.
     * Different {@code Time} instances may use different epochs.
     * This is only useful for measuring and calculating elapsed time against the same Time instance.
     * The return value is guaranteed to be monotonic.
     *
     * <p>When the instance is {@link Time#SYSTEM_TIME} this corresponds to a call to {@link System#nanoTime()}.</p>
     * @return The number of nanoseconds since some unknown epoch.
     */
    long nanoTime();

    /**
     * Causes the current thread to sleep (suspend execution) for the given number of milliseconds
     * plus the given number of nanoseconds, relative to this Time instance and subject to its precision and accuracy.
     * In other words after successful execution of the following code
     * <pre>{@code
     * Time time = ...
     * long start = time.nanoTime();
     * time.sleep(millis, nanos)
     * long end = time.nanoTime();
     * long diff = (1_000_000 * millis + nanos) - (end - start)
     * }</pre>
     * we would expect {@code diff} to be small in magnitude.
     *
     * <p>When the instance is {@link Time#SYSTEM_TIME} this corresponds to a call to {@link Thread#sleep(long, int)}.</p>
     * @param millis The number of milliseconds
     * @param nanos The number of nanoseconds
     * @throws InterruptedException If the sleep was interrupted before the given time has elapsed.
     */
    void sleep(long millis, int nanos) throws InterruptedException;

}

