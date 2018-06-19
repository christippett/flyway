/*
 * Copyright 2010-2018 Boxfuse GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flywaydb.core.internal.database.teradata;

import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.Connection;
import org.flywaydb.core.internal.database.Table;
import org.flywaydb.core.internal.database.Schema;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.internal.exception.FlywaySqlException;

import java.sql.SQLException;
import java.sql.Types;
import java.util.concurrent.Callable;

/**
 * Teradata connection.
 */
public class TeradataConnection extends Connection<TeradataDatabase> {
    TeradataConnection(Configuration configuration, TeradataDatabase database, java.sql.Connection connection
            , boolean originalAutoCommit



    ) {
        super(configuration, database, connection, originalAutoCommit, Types.VARCHAR



        );
    }

    @Override
    public Schema getSchema(String name) {
        return new TeradataSchema(jdbcTemplate, database, name);
    }

    @Override
    protected String getCurrentSchemaNameOrSearchPath() throws SQLException {
        return jdbcTemplate.queryForString("SELECT database");
    }

    @Override
    public <T> T lock(Table table, Callable<T> callable) {
        // Original `lock` method calls the `TransactionTemplate.execute()` method
        // which uses an explicit transaction for the entire migration (not supported by Teradata)
        try {
            table.lock();
            return callable.call();
        } catch (SQLException e) {
            throw new FlywaySqlException("Error locking Teradata table", e);
        } catch (Exception e) {
            RuntimeException rethrow;
            if (e instanceof RuntimeException) {
                rethrow = (RuntimeException) e;
            } else {
                rethrow = new FlywayException(e);
            }
            throw rethrow;
        }
    }
}