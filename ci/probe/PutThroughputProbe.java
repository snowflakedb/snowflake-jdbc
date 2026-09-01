import java.io.File;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

/**
 * SNOW-4039899 throughput probe. Uploads the same ~20 MB file to a client-side-encrypted internal
 * stage three times, once per candidate fix, and prints the wall time of each so we can tell whether
 * the {@code CipherInputStream} 512-byte read is actually the throughput limiter:
 *
 * <ul>
 *   <li>{@code default}  — current driver behaviour (bare BufferedInputStream over the cipher stream)
 *   <li>{@code coalesce} — read-fully wrapper that hands the SDK full-size buffers
 *   <li>{@code tempfile} — drain the encrypted stream to disk and {@code fromFile()} it
 * </ul>
 *
 * The switch is read live by {@code SnowflakeS3Client.upload} via {@code -Dsf.uploadFix}, so all three
 * run in one JVM against one connection. Credentials come from the standard {@code SNOWFLAKE_TEST_*}
 * environment the JDBC CI already exports; key-pair (JWT) auth mirrors the ticket's Domo writeback.
 *
 * <p>Run locally against a slow uplink and all three converge (bandwidth-bound). Run on a fast host
 * (GitHub runner) and, if the trickle is real, {@code default} stays slow while the other two recover.
 */
public class PutThroughputProbe {
  private static final String[] MODES = {"default", "coalesce", "tempfile"};

  public static void main(String[] args) throws Exception {
    long fileBytes = args.length > 0 ? parseSize(args[0]) : 20L * 1024 * 1024;

    File src = new File(System.getProperty("java.io.tmpdir"), "put-probe-payload.dat");
    if (!src.exists() || src.length() != fileBytes) {
      try (RandomAccessFile raf = new RandomAccessFile(src, "rw")) {
        raf.setLength(fileBytes);
      }
    }
    System.out.println("[probe] payload=" + src + " size=" + src.length());

    String host = req("SNOWFLAKE_TEST_HOST");
    String port = env("SNOWFLAKE_TEST_PORT", "443");
    Properties p = new Properties();
    p.put("account", req("SNOWFLAKE_TEST_ACCOUNT"));
    p.put("user", req("SNOWFLAKE_TEST_USER"));
    p.put("db", req("SNOWFLAKE_TEST_DATABASE"));
    p.put("schema", env("SNOWFLAKE_TEST_SCHEMA", "public"));
    p.put("warehouse", req("SNOWFLAKE_TEST_WAREHOUSE"));
    p.put("role", req("SNOWFLAKE_TEST_ROLE"));

    String keyFile = System.getenv("SNOWFLAKE_TEST_PRIVATE_KEY_FILE");
    if (keyFile != null && !keyFile.isEmpty()) {
      String workspace = System.getenv("WORKSPACE");
      String resolved =
          (workspace != null && !new File(keyFile).isAbsolute())
              ? new File(workspace, keyFile).getPath()
              : keyFile;
      p.put("private_key_file", resolved);
      String keyPwd = System.getenv("SNOWFLAKE_TEST_PRIVATE_KEY_PWD");
      if (keyPwd != null && !keyPwd.isEmpty()) {
        p.put("private_key_pwd", keyPwd);
      }
      p.put("authenticator", "SNOWFLAKE_JWT");
      System.out.println("[probe] auth=key-pair keyFile=" + resolved);
    } else {
      p.put("password", req("SNOWFLAKE_TEST_PASSWORD"));
      System.out.println("[probe] auth=password");
    }

    String url = "jdbc:snowflake://" + host + ":" + port;
    Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
    System.out.println("[probe] connecting to " + url + " as " + p.get("user"));

    try (Connection conn = DriverManager.getConnection(url, p);
        Statement st = conn.createStatement()) {
      st.execute("CREATE OR REPLACE TEMPORARY STAGE put_probe_stage");
      System.out.println("[probe] stage ready; internal stage => client-side encrypted");

      long[] millis = new long[MODES.length];
      for (int i = 0; i < MODES.length; i++) {
        String mode = MODES[i];
        if ("default".equals(mode)) {
          System.clearProperty("sf.uploadFix");
        } else {
          System.setProperty("sf.uploadFix", mode);
        }
        String put =
            "PUT file://"
                + src.getAbsolutePath()
                + " @put_probe_stage auto_compress=false overwrite=true";
        long t0 = System.currentTimeMillis();
        st.execute(put);
        millis[i] = System.currentTimeMillis() - t0;
        double rate = src.length() / 1024.0 / (millis[i] / 1000.0);
        System.out.printf(
            "[probe] RESULT mode=%-8s ms=%-8d rate=%.1f KB/s%n", mode, millis[i], rate);
      }

      System.out.println("\n[probe] ===== SUMMARY (20 MB client-side-encrypted PUT) =====");
      for (int i = 0; i < MODES.length; i++) {
        System.out.printf("[probe]   %-8s %,d ms%n", MODES[i], millis[i]);
      }
      double best = Double.MAX_VALUE, worst = 0;
      for (long m : millis) {
        best = Math.min(best, m);
        worst = Math.max(worst, m);
      }
      System.out.printf(
          "[probe]   spread: worst/best = %.2fx  => %s%n",
          worst / best,
          worst / best > 3.0
              ? "client-side path IS the limiter (trickle confirmed)"
              : "all modes converge => bandwidth-bound, trickle NOT the limiter");
    }
  }

  private static String env(String k, String def) {
    String v = System.getenv(k);
    return (v == null || v.isEmpty()) ? def : v;
  }

  private static String req(String k) {
    String v = System.getenv(k);
    if (v == null || v.isEmpty()) {
      throw new IllegalStateException("missing required env " + k);
    }
    return v;
  }

  private static long parseSize(String s) {
    s = s.trim().toUpperCase();
    long mult = 1;
    if (s.endsWith("G")) {
      mult = 1024L * 1024 * 1024;
      s = s.substring(0, s.length() - 1);
    } else if (s.endsWith("M")) {
      mult = 1024L * 1024;
      s = s.substring(0, s.length() - 1);
    }
    return (long) (Double.parseDouble(s) * mult);
  }
}
