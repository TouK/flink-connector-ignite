package pl.touk.flink.ignite.dialect;

import org.apache.flink.connector.jdbc.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.dialect.mysql.MySqlDialect;
import org.apache.flink.connector.jdbc.dialect.psql.PostgresDialect;
import org.apache.flink.table.types.logical.RowType;
import pl.touk.flink.ignite.converter.IgniteRowConverter;

import java.util.Optional;

// what matters is that it implements org.apache.flink.connector.jdbc.dialect.JdbcDialect interface, concrete implementation used
// as base class to have some method implemented
public class IgniteDialect extends PostgresDialect {
    private static final long serialVersionUID = 1L;

    @Override
    public String dialectName() {
        return "Ignite";
    }

    @Override
    public JdbcRowConverter getRowConverter(RowType rowType) {
        return new IgniteRowConverter(rowType);
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("org.apache.ignite.IgniteJdbcThinDriver");
    }
}
