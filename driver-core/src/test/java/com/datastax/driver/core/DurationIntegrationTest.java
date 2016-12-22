/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;

@CassandraVersion(major = 3.10)
public class DurationIntegrationTest extends CCMTestsSupport {

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TABLE test_duration (pk int PRIMARY KEY, c1 duration)");
    }

    /**
     * Validates that columns using the duration type are properly handled by the driver in the following ways:
     * read, write, and column metadata.
     *
     * @jira_ticket JAVA-1347
     * @test_category metadata
     */
    @Test(groups = "short")
    public void should_serialize_and_deserialize_durations() {
        // read and write
        Duration expected = Duration.from("5mo6d7ns");
        session().execute("INSERT INTO test_duration (pk, c1) VALUES (2, ?)", expected);
        Row row = session().execute("SELECT c1 from test_duration WHERE pk = 2").one();
        Duration actual = row.get("c1", Duration.class);
        assertThat(actual).isEqualTo(expected);
        // column metadata
        TableMetadata table = cluster().getMetadata().getKeyspace(keyspace).getTable("test_duration");
        assertThat(table.getColumn("c1")).hasType(DataType.duration());
    }

}
