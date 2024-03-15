package net.snowflake.client.jdbc.diagnostic;

import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

public class TcpDiagnosticCheck extends DiagnosticCheck {

    private static final SFLogger logger =
            SFLoggerFactory.getLogger(TcpDiagnosticCheck.class);

    public TcpDiagnosticCheck(){
        super("TCP Connection Diagnostic Test");
    }

}
