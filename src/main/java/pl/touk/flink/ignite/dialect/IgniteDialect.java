package pl.touk.flink.ignite.dialect;

import org.apache.flink.connector.jdbc.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.dialect.AbstractDialect;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import pl.touk.flink.ignite.converter.IgniteRowConverter;

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

// TODO_PAWEL jest problem, jak sie zamieni wszystkie metody na throw runtime exception oprocz quoteIdentifier i getRowConverter to
// testy dalej przechodza
public class IgniteDialect extends AbstractDialect {
    private static final long serialVersionUID = 1L;

    // value chosen based on https://ignite.apache.org/docs/latest/sql-reference/data-types#timestamp
    private static final int MAX_TIMESTAMP_PRECISION = 9;
    private static final int MIN_TIMESTAMP_PRECISION = 1;
    private static final int MAX_DECIMAL_PRECISION = 1000;
    private static final int MIN_DECIMAL_PRECISION = 1;

    @Override
    public String dialectName() {
        return "Ignite";
    }

    @Override
    public JdbcRowConverter getRowConverter(RowType rowType) {
        return new IgniteRowConverter(rowType);
    }

    @Override
    public String getLimitClause(long l) {
        return "LIMIT " + l;
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("org.apache.ignite.IgniteJdbcThinDriver");
    }

    // not supported for now, probably merge can be used https://ignite.apache.org/docs/latest/sql-reference/dml#merge
    @Override
    public Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        return Optional.empty();
    }

    @Override
    public Optional<Range> decimalPrecisionRange() {
        return Optional.of(Range.of(MIN_DECIMAL_PRECISION, MAX_DECIMAL_PRECISION));
    }

    @Override
    public Optional<Range> timestampPrecisionRange() {
        return Optional.of(Range.of(MIN_TIMESTAMP_PRECISION, MAX_TIMESTAMP_PRECISION));
    }

    @Override
    public Set<LogicalTypeRoot> supportedTypes() {
        return EnumSet.of(
                LogicalTypeRoot.CHAR,
                LogicalTypeRoot.VARCHAR,
                LogicalTypeRoot.BOOLEAN,
                LogicalTypeRoot.VARBINARY,
                LogicalTypeRoot.DECIMAL,
                LogicalTypeRoot.TINYINT,
                LogicalTypeRoot.SMALLINT,
                LogicalTypeRoot.INTEGER,
                LogicalTypeRoot.BIGINT,
                LogicalTypeRoot.FLOAT,
                LogicalTypeRoot.DOUBLE,
                LogicalTypeRoot.DATE,
                LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE,
                LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE,
                LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                LogicalTypeRoot.ARRAY);
    }
}
