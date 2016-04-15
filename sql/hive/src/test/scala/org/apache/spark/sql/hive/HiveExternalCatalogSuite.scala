/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.VersionInfo

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.hive.client.{HiveClient, IsolatedClientLoader}

/**
 * Test suite for the [[HiveExternalCatalog]].
 */
class HiveExternalCatalogSuite extends CatalogTestCases {

  private lazy val client: HiveClient = {
    IsolatedClientLoader.forVersion(
      hiveMetastoreVersion = HiveContext.hiveExecutionVersion,
      hadoopVersion = VersionInfo.getVersion,
      sparkConf = new SparkConf(),
      hadoopConf = new Configuration()).createClient()
  }

  protected override val utils: CatalogTestUtils = new CatalogTestUtils {
    override val tableInputFormat: String = "org.apache.hadoop.mapred.SequenceFileInputFormat"
    override val tableOutputFormat: String = "org.apache.hadoop.mapred.SequenceFileOutputFormat"
    override def newEmptyCatalog(): ExternalCatalog = new HiveExternalCatalog(client)
  }

  protected override def resetState(): Unit = client.reset()

  import utils._

  test("drop database cascade with function defined") {
    import org.apache.spark.sql.catalyst.expressions.Lower

    val catalog = newEmptyCatalog()
    val dbName = "dbCascade"
    val path = newUriForDatabase()
    catalog.createDatabase(CatalogDatabase(dbName, "", path, Map.empty), ignoreIfExists = false)
    // create a permanent function in catalog
    catalog.createFunction(dbName, CatalogFunction(
      FunctionIdentifier("func1", Some(dbName)), classOf[Lower].getName, Nil))
    assert(catalog.functionExists(dbName, "func1"))
    catalog.dropDatabase(dbName, ignoreIfNotExists = false, cascade = true)
    // create the db again because `functionExists` requires db to exist
    catalog.createDatabase(CatalogDatabase(dbName, "", path, Map.empty), ignoreIfExists = false)
    assert(!catalog.functionExists(dbName, "func1"))
    catalog.dropDatabase(dbName, ignoreIfNotExists = true, cascade = true)
  }
}
