package net.snowflake.client.loader;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import org.junit.jupiter.api.Test;

public class OnErrorTest {
  @Test
  public void testValidate() {
    // positive
    assertThat(OnError.validate("ABORT_STATEMENT"), is(Boolean.TRUE));
    assertThat(OnError.validate("ABORT_STATeMENT"), is(Boolean.TRUE));
    assertThat(OnError.validate("CONTINUE"), is(Boolean.TRUE));
    assertThat(OnError.validate("cOnTinue"), is(Boolean.TRUE));
    assertThat(OnError.validate("SKIP_FILE"), is(Boolean.TRUE));
    assertThat(OnError.validate("Skip_File_10"), is(Boolean.TRUE));

    // Out of range error should be detected by the server.
    assertThat(OnError.validate("Skip_File_123%"), is(Boolean.TRUE));

    // negative
    assertThat(OnError.validate(null), is(Boolean.FALSE));
    assertThat(OnError.validate(""), is(Boolean.FALSE));
    assertThat(OnError.validate("Bogus"), is(Boolean.FALSE));
    assertThat(OnError.validate("abortstatement"), is(Boolean.FALSE));
    assertThat(OnError.validate("continues"), is(Boolean.FALSE));
    assertThat(OnError.validate("skipfile"), is(Boolean.FALSE));
    assertThat(OnError.validate("skip_file_abcdef"), is(Boolean.FALSE));
    assertThat(OnError.validate("skip_file_abcdef%"), is(Boolean.FALSE));
    assertThat(OnError.validate("skip_file_25.0%"), is(Boolean.FALSE));
    assertThat(OnError.validate("skip_file_-10%"), is(Boolean.FALSE));
    assertThat(OnError.validate("continue; delete from t"), is(Boolean.FALSE));
    assertThat(OnError.validate("ABORT_STATEMENT; drop table t"), is(Boolean.FALSE));
  }
}
