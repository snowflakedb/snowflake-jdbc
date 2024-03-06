package net.snowflake.client.util;

/**
 * Stopwatch class used to calculate the time between start and stop.
 */
public class Stopwatch {
    private boolean isStarted = false;
    private Long startTime;
    private Long stopTime;

    /**
     * Starts the Stopwatch.
     *
     * @throws IllegalStateException when Stopwatch is already running.
     */
    public void start() {
        if (isStarted) {
            throw new IllegalStateException("Stopwatch is already running");
        }

        isStarted = true;
        startTime = System.nanoTime();
    }

    /**
     * Stops the Stopwatch.
     *
     * @throws IllegalStateException when Stopwatch was not yet started or is already stopped.
     */
    public void stop() {
        if (!isStarted) {
            if (startTime == null) {
                throw new IllegalStateException("Stopwatch has not been started");
            }
            throw new IllegalStateException("Stopwatch is already stopped");
        }

        isStarted = false;
        stopTime = System.nanoTime();
    }

    /**
     * Resets the instance to it's initial state.
     */
    public void reset() {
        isStarted = false;
        startTime = null;
        stopTime = null;
    }

    /**
     * Restarts the instance.
     */
    public void restart() {
        isStarted = true;
        startTime = System.nanoTime();
        stopTime = null;
    }

    /**
     * Get the elapsed time (in ms) between the stopTime and startTime.
     *
     * @return elapsed milliseconds between stopTime and startTime
     * @throws IllegalStateException when Stopwatch has not been started yet
     */
    public long elapsedMillis() {
        return elapsedNanos() / 1_000_000;
    }

    /**
     * Get the elapsed time (in nanoseconds) between the stopTime and startTime.
     *
     * @return elapsed nanoseconds between stopTime and startTime
     * @throws IllegalStateException when Stopwatch has not been started yet
     */
    public long elapsedNanos() {
        if (isStarted) {
            return (System.nanoTime() - startTime);
        }

        if (stopTime == null) {
            throw new IllegalStateException("Stopwatch has not been ran yet");
        }

        return stopTime - startTime;
    }

    /**
     * Get the instance status.
     *
     * @return true if the stopwatch is running, false otherwise
     */
    public boolean isStarted() {
        return isStarted;
    }
}
