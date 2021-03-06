/*
 * Copyright 2020 Aiven Oy
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.connect.jdbc.sink;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.connect.jdbc.dialect.DatabaseDialect;
import io.aiven.connect.jdbc.dialect.DatabaseDialect.StatementBinder;
import io.aiven.connect.jdbc.sink.metadata.FieldsMetadata;
import io.aiven.connect.jdbc.sink.metadata.SchemaPair;
import io.aiven.connect.jdbc.util.ColumnId;
import io.aiven.connect.jdbc.util.TableDefinition;
import io.aiven.connect.jdbc.util.TableId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.aiven.connect.jdbc.sink.JdbcSinkConfig.InsertMode.INSERT;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class BufferedRecords {
    private static final Logger log = LoggerFactory.getLogger(BufferedRecords.class);

    private final TableId tableId;
    private final JdbcSinkConfig config;
    private final DatabaseDialect dbDialect;
    private final DbStructure dbStructure;
    private final Connection connection;

    private List<SinkRecord> records = new ArrayList<>();
    private Schema keySchema;
    private Schema valueSchema;
    private FieldsMetadata fieldsMetadata;
    private PreparedStatement updatePreparedStatement;
    private PreparedStatement deletePreparedStatement;
    private StatementBinder updateStatementBinder;
    private StatementBinder deleteStatementBinder;
    private boolean deletesInBatch = false;

    public BufferedRecords(
            final JdbcSinkConfig config,
            final TableId tableId,
            final DatabaseDialect dbDialect,
            final DbStructure dbStructure,
            final Connection connection
    ) {
        this.tableId = tableId;
        this.config = config;
        this.dbDialect = dbDialect;
        this.dbStructure = dbStructure;
        this.connection = connection;
    }

    public List<SinkRecord> add(final SinkRecord record) throws SQLException {
        final List<SinkRecord> flushed = new ArrayList<>();

        boolean schemaChanged = false;
        if (!Objects.equals(keySchema, record.keySchema())) {
            keySchema = record.keySchema();
            schemaChanged = true;
        }
        if (isNull(record.valueSchema())) {
            // For deletes, both the value and value schema come in as null.
            // We don't want to treat this as a schema change if key schemas is the same
            // otherwise we flush unnecessarily.
            if (config.deleteEnabled) {
                deletesInBatch = true;
            }
        } else if (Objects.equals(valueSchema, record.valueSchema())) {
            if (config.deleteEnabled && deletesInBatch) {
                // flush so an insert after a delete of same record isn't lost
                flushed.addAll(flush());
            }
        } else {
            // value schema is not null and has changed. This is a real schema change.
            valueSchema = record.valueSchema();
            schemaChanged = true;
        }

        if (schemaChanged) {
            // Each batch needs to have the same schemas, so get the buffered records out
            flushed.addAll(flush());

            // re-initialize everything that depends on the record schema
            final SchemaPair schemaPair = new SchemaPair(
                    record.keySchema(),
                    record.valueSchema()
            );
            fieldsMetadata = FieldsMetadata.extract(
                    tableId.tableName(),
                    config.pkMode,
                    config.pkFields,
                    config.fieldsWhitelist,
                    schemaPair
            );
            dbStructure.createOrAmendIfNecessary(
                    config,
                    connection,
                    tableId,
                    fieldsMetadata
            );
            final TableDefinition tableDefinition = dbStructure.tableDefinitionFor(tableId, connection);
            final String insertSql = getInsertSql(tableDefinition);
            final String deleteSql = getDeleteSql();
            log.debug(
                    "{} sql: {} deleteSql: {} meta: {}",
                    config.insertMode,
                    insertSql,
                    deleteSql,
                    fieldsMetadata
            );
            close();
            // should this be using dbDialect.createPreparedStatement ?
            updatePreparedStatement = connection.prepareStatement(insertSql);
            updateStatementBinder = dbDialect.statementBinder(
                    updatePreparedStatement,
                    config.pkMode,
                    schemaPair,
                    fieldsMetadata,
                    config.insertMode
            );
            if (config.deleteEnabled && nonNull(deleteSql)) {
                deletePreparedStatement = connection.prepareStatement(deleteSql);
                deleteStatementBinder = dbDialect.statementBinder(
                        deletePreparedStatement,
                        config.pkMode,
                        schemaPair,
                        fieldsMetadata,
                        config.insertMode
                );
            }
        }
        records.add(record);

        if (records.size() >= config.batchSize) {
            flushed.addAll(flush());
        }
        return flushed;
    }

    public List<SinkRecord> flush() throws SQLException {
        if (records.isEmpty()) {
            log.debug("Records is empty");
            return new ArrayList<>();
        }
        log.debug("Flushing {} buffered records", records.size());
        for (SinkRecord record : records) {
            if (isNull(record.value()) && nonNull(deleteStatementBinder)) {
                deleteStatementBinder.bindRecord(record);
            } else {
                updateStatementBinder.bindRecord(record);
            }
        }
        Optional<Long> totalUpdateCount = executeUpdates();
        long totalDeleteCount = executeDeletes();

        final long expectedCount = updateRecordCount();
        log.trace("{} records:{} resulting in totalUpdateCount:{} totalDeleteCount:{}",
                config.insertMode, records.size(), totalUpdateCount, totalDeleteCount);
        if (totalUpdateCount.filter(total -> total != expectedCount).isPresent()
                && config.insertMode == INSERT) {
            throw new ConnectException(String.format(
                    "Update count (%d) did not sum up to total number of records inserted (%d)",
                    totalUpdateCount.get(),
                    expectedCount));
        }
        if (!totalUpdateCount.isPresent()) {
            log.info(
                    "{} records:{} , but no count of the number of rows it affected is available",
                    config.insertMode,
                    records.size()
            );
        }

        final List<SinkRecord> flushedRecords = records;
        records = new ArrayList<>();
        deletesInBatch = false;
        return flushedRecords;
    }

    /**
     * @return an optional count of all updated rows or an empty optional if no info is available
     */
    private Optional<Long> executeUpdates() throws SQLException {
        Optional<Long> count = Optional.empty();
        for (int updateCount : updatePreparedStatement.executeBatch()) {
            if (updateCount != Statement.SUCCESS_NO_INFO) {
                count = count.isPresent()
                        ? count.map(total -> total + updateCount)
                        : Optional.of((long)updateCount);
            }
        }
        return count;
    }

    private long executeDeletes() throws SQLException {
        long totalDeleteCount = 0;
        if (nonNull(deletePreparedStatement)) {
            for (int updateCount : deletePreparedStatement.executeBatch()) {
                if (updateCount != Statement.SUCCESS_NO_INFO) {
                    totalDeleteCount += updateCount;
                }
            }
        }
        return totalDeleteCount;
    }

    private long updateRecordCount() {
        return records
                .stream()
                // ignore deletes
                .filter(record -> nonNull(record.value()) || !config.deleteEnabled)
                .count();
    }

    public void close() throws SQLException {
        log.debug(
                "Closing BufferedRecords with updatePreparedStatement: {} deletePreparedStatement: {}",
                updatePreparedStatement,
                deletePreparedStatement
        );
        if (nonNull(updatePreparedStatement)) {
            updatePreparedStatement.close();
            updatePreparedStatement = null;
        }
        if (nonNull(deletePreparedStatement)) {
            deletePreparedStatement.close();
            deletePreparedStatement = null;
        }
    }

    private String getInsertSql(final TableDefinition tableDefinition) {
        switch (config.insertMode) {
            case INSERT:
                return dbDialect.buildInsertStatement(
                        tableId,
                        tableDefinition,
                        asColumns(fieldsMetadata.keyFieldNames),
                        asColumns(fieldsMetadata.nonKeyFieldNames)
                );
            case UPSERT:
                if (fieldsMetadata.keyFieldNames.isEmpty()) {
                    throw new ConnectException(String.format(
                            "Write to table '%s' in UPSERT mode requires key field names to be known, check the"
                                    + " primary key configuration",
                            tableId
                    ));
                }
                try {
                    return dbDialect.buildUpsertQueryStatement(
                            tableId,
                            tableDefinition,
                            asColumns(fieldsMetadata.keyFieldNames),
                            asColumns(fieldsMetadata.nonKeyFieldNames)
                    );
                } catch (final UnsupportedOperationException e) {
                    throw new ConnectException(String.format(
                            "Write to table '%s' in UPSERT mode is not supported with the %s dialect.",
                            tableId,
                            dbDialect.name()
                    ));
                }
            case UPDATE:
                return dbDialect.buildUpdateStatement(
                        tableId,
                        tableDefinition,
                        asColumns(fieldsMetadata.keyFieldNames),
                        asColumns(fieldsMetadata.nonKeyFieldNames)
                );
            default:
                throw new ConnectException("Invalid insert mode");
        }
    }

    private String getDeleteSql() {
        String sql = null;
        if (config.deleteEnabled) {
            switch (config.pkMode) {
                case RECORD_KEY:
                    if (fieldsMetadata.keyFieldNames.isEmpty()) {
                        throw new ConnectException("Require primary keys to support delete");
                    }
                    sql = dbDialect.buildDeleteStatement(
                            tableId,
                            asColumns(fieldsMetadata.keyFieldNames)
                    );
                    break;
                default:
                    throw new ConnectException("Deletes are only supported for pk.mode record_key");
            }
        }
        return sql;
    }

    private Collection<ColumnId> asColumns(final Collection<String> names) {
        return names.stream()
                .map(name -> new ColumnId(tableId, name))
                .collect(Collectors.toList());
    }
}
