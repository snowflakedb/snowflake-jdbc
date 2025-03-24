package net.snowflake.client.jdbc;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import net.snowflake.client.AbstractDriverIT;

public class SnowflakeDriverConnectionStressTest {
  private static final String QUERY = "select current_user()";

  public static void main(String[] args) throws InterruptedException, ExecutionException {
    final int run_millis = 2 * 60 * 1000;
    final int dop = 6;
    final int queries_per_connection = 50;
    doTest(run_millis, dop, queries_per_connection);
  }

  private static void doTest(int run_millis, int dop, final int num_queries)
      throws InterruptedException, ExecutionException {
    // make sure we can do basic single threaded
    doConnectWithQuery(1);

    final ExecutorService executorService = Executors.newFixedThreadPool(dop);

    final AtomicBoolean running = new AtomicBoolean(true);

    Runnable r =
        new Runnable() {
          @Override
          public void run() {
            while (running.get()) {
              doConnectWithQuery(num_queries);
            }
          }
        };

    List<Future<?>> futures = new ArrayList<>(dop);

    for (int i = 0; i < dop; i++) {
      final Future<?> f = executorService.submit(r);
      futures.add(f);
    }

    Thread.sleep(run_millis);

    running.set(false);

    for (Future<?> future : futures) {
      // doing this to get any errors
      future.get();
    }

    executorService.shutdownNow();
    executorService.awaitTermination(1, TimeUnit.SECONDS);
  }

  private static void doConnectWithQuery(int num_queries) {
    try {
      final long before = System.currentTimeMillis();
      connectAndQuery(num_queries);
      final long diff = System.currentTimeMillis() - before;
      say("connect_with_queries_millis=" + diff);
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  private static void connectAndQuery(int num_queries) throws SQLException {

    try (Connection connection = AbstractDriverIT.getConnection();
        Statement statement = connection.createStatement()) {

      for (int i = 0; i < num_queries; i++) {

        try (ResultSet resultSet = statement.executeQuery(QUERY)) {
          while (resultSet.next()) {
            final String user = resultSet.getString(1);
            assertNotNull(user);
          }
        }
      }
    }
  }

  protected static void say(String arg) {
    System.out.println(
        System.currentTimeMillis() + ":" + Thread.currentThread().getId() + " " + arg);
  }
}
