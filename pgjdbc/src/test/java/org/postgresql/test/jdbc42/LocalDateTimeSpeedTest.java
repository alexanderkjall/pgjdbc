package org.postgresql.test.jdbc42;

import static org.junit.Assert.assertEquals;

import org.postgresql.test.TestUtil;
import org.postgresql.test.jdbc2.BaseTest4;

import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class LocalDateTimeSpeedTest extends BaseTest4 {
  //test times pre-optimization:
  //1m 0s 52ms
  //1m 11s 414ms
  //1m 19s 340ms
  //1m 27s 274ms
  //1m 20s 276ms
  //1m 27s 212ms
  //1m 29s 242ms
  //test times post-optimization:
  //1m 16s 341ms
  //1m 2s 802ms
  //1m 5s 410ms
  //1m 1s 401ms
  //1m 4s 200ms
  //56s 993ms
  //1m 3s 92ms
  @Test
  public void testLocalDateTimeSerialization() throws InterruptedException {
    ExecutorService tp = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 4);
    for (int i = 0; i < Runtime.getRuntime().availableProcessors() * 4; i++) {
      tp.submit(() -> {
        System.out.println("starting query thread");
        try {
          Properties props = new Properties();
          updateProperties(props);
          Connection con = TestUtil.openDB(props);

          PreparedStatement stmt = con.prepareStatement("select ?");

          for (int j = 0; j < 100000; j++) {
            LocalDateTime now = LocalDateTime.now().withNano(0);
            stmt.setObject(1, now);
            ResultSet rs = stmt.executeQuery();
            rs.next();
            assertEquals(now, rs.getObject(1, LocalDateTime.class));
          }
        } catch (Throwable t) {
          t.printStackTrace();
          throw new RuntimeException(t);
        }
      });
    }

    tp.shutdownNow();
    if (!tp.awaitTermination(10, TimeUnit.MINUTES)) {
      throw new RuntimeException("test did not complete within time limit");
    }
  }
}
