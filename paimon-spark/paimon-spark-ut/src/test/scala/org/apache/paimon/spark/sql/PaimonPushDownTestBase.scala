/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.spark.sql

import org.apache.paimon.spark.{PaimonScan, PaimonSparkTestBase, SparkTable}
import org.apache.paimon.table.source.DataSplit

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.trees.TreePattern.DYNAMIC_PRUNING_SUBQUERY
import org.apache.spark.sql.connector.read.{ScanBuilder, SupportsPushDownLimit}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.junit.jupiter.api.Assertions

abstract class PaimonPushDownTestBase extends PaimonSparkTestBase {

  import testImplicits._

  test(s"Paimon push down: apply partition filter push down with non-partitioned table") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, pt STRING)
                 |TBLPROPERTIES ('primary-key'='id, pt', 'bucket'='2')
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', 'p1'), (2, 'b', 'p1'), (3, 'c', 'p2')")

    val q = "SELECT * FROM T WHERE pt = 'p1'"
    Assertions.assertTrue(checkEqualToFilterExists(q, "pt", Literal("p1")))
    checkAnswer(spark.sql(q), Row(1, "a", "p1") :: Row(2, "b", "p1") :: Nil)
  }

  test(s"Paimon push down: apply partition filter push down with partitioned table") {
    spark.sql(s"""
                 |CREATE TABLE T (id INT, name STRING, pt STRING)
                 |TBLPROPERTIES ('primary-key'='id, pt', 'bucket'='2')
                 |PARTITIONED BY (pt)
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', 'p1'), (2, 'b', 'p1'), (3, 'c', 'p2'), (4, 'd', 'p3')")

    // partition filter push down did not hit cases:
    // case 1
    var q = "SELECT * FROM T WHERE id = '1'"
    Assertions.assertTrue(checkFilterExists(q))
    checkAnswer(spark.sql(q), Row(1, "a", "p1") :: Nil)

    // case 2
    // filter "id = '1' or pt = 'p1'" can't push down completely, it still needs to be evaluated after scanning
    q = "SELECT * FROM T WHERE id = '1' or pt = 'p1'"
    Assertions.assertTrue(checkEqualToFilterExists(q, "pt", Literal("p1")))
    checkAnswer(spark.sql(q), Row(1, "a", "p1") :: Row(2, "b", "p1") :: Nil)

    // partition filter push down hit cases:
    // case 1
    q = "SELECT * FROM T WHERE pt = 'p1'"
    Assertions.assertFalse(checkFilterExists(q))
    checkAnswer(spark.sql(q), Row(1, "a", "p1") :: Row(2, "b", "p1") :: Nil)

    // case 2
    q = "SELECT * FROM T WHERE id = '1' and pt = 'p1'"
    Assertions.assertFalse(checkEqualToFilterExists(q, "pt", Literal("p1")))
    checkAnswer(spark.sql(q), Row(1, "a", "p1") :: Nil)

    // case 3
    q = "SELECT * FROM T WHERE pt < 'p3'"
    Assertions.assertFalse(checkFilterExists(q))
    checkAnswer(spark.sql(q), Row(1, "a", "p1") :: Row(2, "b", "p1") :: Row(3, "c", "p2") :: Nil)
  }

  test("Paimon pushDown: limit for append-only tables with deletion vector") {
    withTable("dv_test") {
      spark.sql(
        """
          |CREATE TABLE dv_test (c1 INT, c2 STRING)
          |TBLPROPERTIES ('deletion-vectors.enabled' = 'true', 'source.split.target-size' = '1')
          |""".stripMargin)

      spark.sql("insert into table dv_test values(1, 'a'),(2, 'b'),(3, 'c')")
      assert(spark.sql("select * from dv_test limit 2").count() == 2)

      spark.sql("delete from dv_test where c1 = 1")
      assert(spark.sql("select * from dv_test limit 2").count() == 2)
    }
  }

  test("Paimon pushDown: limit for append-only tables") {
    assume(gteqSpark3_3)
    spark.sql(s"""
                 |CREATE TABLE T (a INT, b STRING, c STRING)
                 |PARTITIONED BY (c)
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', '11'), (2, 'b', '11')")
    spark.sql("INSERT INTO T VALUES (3, 'c', '22'), (4, 'd', '22')")

    checkAnswer(
      spark.sql("SELECT * FROM T ORDER BY a"),
      Row(1, "a", "11") :: Row(2, "b", "11") :: Row(3, "c", "22") :: Row(4, "d", "22") :: Nil)

    val scanBuilder = getScanBuilder()
    Assertions.assertTrue(scanBuilder.isInstanceOf[SupportsPushDownLimit])

    val dataSplitsWithoutLimit = scanBuilder.build().asInstanceOf[PaimonScan].getOriginSplits
    Assertions.assertTrue(dataSplitsWithoutLimit.length >= 2)

    // It still returns false even it can push down limit.
    Assertions.assertFalse(scanBuilder.asInstanceOf[SupportsPushDownLimit].pushLimit(1))
    val dataSplitsWithLimit = scanBuilder.build().asInstanceOf[PaimonScan].getOriginSplits
    Assertions.assertEquals(1, dataSplitsWithLimit.length)

    Assertions.assertEquals(1, spark.sql("SELECT * FROM T LIMIT 1").count())
  }

  test("Paimon pushDown: limit for primary key table") {
    assume(gteqSpark3_3)
    spark.sql(s"""
                 |CREATE TABLE T (a INT, b STRING, c STRING)
                 |TBLPROPERTIES ('primary-key'='a')
                 |""".stripMargin)

    spark.sql("INSERT INTO T VALUES (1, 'a', '11'), (2, 'b', '22')")
    spark.sql("INSERT INTO T VALUES (3, 'c', '11'), (4, 'd', '22')")

    val scanBuilder = getScanBuilder()
    Assertions.assertTrue(scanBuilder.isInstanceOf[SupportsPushDownLimit])

    // Case 1: All dataSplits is rawConvertible.
    val dataSplitsWithoutLimit = scanBuilder.build().asInstanceOf[PaimonScan].getOriginSplits
    Assertions.assertEquals(4, dataSplitsWithoutLimit.length)
    // All dataSplits is rawConvertible.
    dataSplitsWithoutLimit.foreach(
      splits => {
        Assertions.assertTrue(splits.asInstanceOf[DataSplit].rawConvertible())
      })

    // It still returns false even it can push down limit.
    Assertions.assertFalse(scanBuilder.asInstanceOf[SupportsPushDownLimit].pushLimit(1))
    val dataSplitsWithLimit = scanBuilder.build().asInstanceOf[PaimonScan].getOriginSplits
    Assertions.assertEquals(1, dataSplitsWithLimit.length)
    Assertions.assertEquals(1, spark.sql("SELECT * FROM T LIMIT 1").count())

    Assertions.assertFalse(scanBuilder.asInstanceOf[SupportsPushDownLimit].pushLimit(2))
    val dataSplitsWithLimit1 = scanBuilder.build().asInstanceOf[PaimonScan].getOriginSplits
    Assertions.assertEquals(2, dataSplitsWithLimit1.length)
    Assertions.assertEquals(2, spark.sql("SELECT * FROM T LIMIT 2").count())

    // Case 2: Update 2 rawConvertible dataSplits to convert to nonRawConvertible.
    spark.sql("INSERT INTO T VALUES (1, 'a2', '11'), (2, 'b2', '22')")
    val scanBuilder2 = getScanBuilder()
    val dataSplitsWithoutLimit2 = scanBuilder2.build().asInstanceOf[PaimonScan].getOriginSplits
    Assertions.assertEquals(4, dataSplitsWithoutLimit2.length)
    // Now, we have 4 dataSplits, and 2 dataSplit is nonRawConvertible, 2 dataSplit is rawConvertible.
    Assertions.assertEquals(
      2,
      dataSplitsWithoutLimit2.count(split => { split.asInstanceOf[DataSplit].rawConvertible() }))

    // Return 2 dataSplits.
    Assertions.assertFalse(scanBuilder2.asInstanceOf[SupportsPushDownLimit].pushLimit(2))
    val dataSplitsWithLimit2 = scanBuilder2.build().asInstanceOf[PaimonScan].getOriginSplits
    Assertions.assertEquals(2, dataSplitsWithLimit2.length)
    Assertions.assertEquals(2, spark.sql("SELECT * FROM T LIMIT 2").count())

    // 2 dataSplits cannot meet the limit requirement, so need to scan all dataSplits.
    Assertions.assertFalse(scanBuilder2.asInstanceOf[SupportsPushDownLimit].pushLimit(3))
    val dataSplitsWithLimit22 = scanBuilder2.build().asInstanceOf[PaimonScan].getOriginSplits
    // Need to scan all dataSplits.
    Assertions.assertEquals(4, dataSplitsWithLimit22.length)
    Assertions.assertEquals(3, spark.sql("SELECT * FROM T LIMIT 3").count())

    // Case 3: Update the remaining 2 rawConvertible dataSplits to make all dataSplits is nonRawConvertible.
    spark.sql("INSERT INTO T VALUES (3, 'c', '11'), (4, 'd', '22')")
    val scanBuilder3 = getScanBuilder()
    val dataSplitsWithoutLimit3 = scanBuilder3.build().asInstanceOf[PaimonScan].getOriginSplits
    Assertions.assertEquals(4, dataSplitsWithoutLimit3.length)

    // All dataSplits is nonRawConvertible.
    dataSplitsWithoutLimit3.foreach(
      splits => {
        Assertions.assertFalse(splits.asInstanceOf[DataSplit].rawConvertible())
      })

    Assertions.assertFalse(scanBuilder3.asInstanceOf[SupportsPushDownLimit].pushLimit(1))
    val dataSplitsWithLimit3 = scanBuilder3.build().asInstanceOf[PaimonScan].getOriginSplits
    // Need to scan all dataSplits.
    Assertions.assertEquals(4, dataSplitsWithLimit3.length)
    Assertions.assertEquals(1, spark.sql("SELECT * FROM T LIMIT 1").count())
  }

  test("Paimon pushDown: limit for table with deletion vector") {
    assume(gteqSpark3_3)
    Seq(true, false).foreach(
      deletionVectorsEnabled => {
        Seq(true, false).foreach(
          primaryKeyTable => {
            withTable("T") {
              sql(s"""
                     |CREATE TABLE T (id INT)
                     |TBLPROPERTIES (
                     | 'deletion-vectors.enabled' = $deletionVectorsEnabled,
                     | '${if (primaryKeyTable) "primary-key" else "bucket-key"}' = 'id',
                     | 'bucket' = '10'
                     |)
                     |""".stripMargin)

              sql("INSERT INTO T SELECT id FROM range (1, 50000)")
              sql("DELETE FROM T WHERE id % 13 = 0")

              val withoutLimit = getScanBuilder().build().asInstanceOf[PaimonScan].getOriginSplits
              assert(withoutLimit.length == 10)

              val scanBuilder = getScanBuilder().asInstanceOf[SupportsPushDownLimit]
              scanBuilder.pushLimit(1)
              val withLimit = scanBuilder.build().asInstanceOf[PaimonScan].getOriginSplits
              if (deletionVectorsEnabled || !primaryKeyTable) {
                assert(withLimit.length == 1)
              } else {
                assert(withLimit.length == 10)
              }
            }
          })
      })
  }

  test("Paimon pushDown: runtime filter") {
    withTable("source", "t") {
      Seq((1L, "x1", "2023"), (2L, "x2", "2023"), (5L, "x5", "2025"), (6L, "x6", "2026"))
        .toDF("a", "b", "pt")
        .createOrReplaceTempView("source")

      spark.sql("""
                  |CREATE TABLE t (id INT, name STRING, pt STRING) PARTITIONED BY (pt)
                  |""".stripMargin)

      spark.sql(
        """
          |INSERT INTO t VALUES (1, "a", "2023"), (3, "c", "2023"), (5, "e", "2025"), (7, "g", "2027")
          |""".stripMargin)

      val df1 = spark.sql("""
                            |SELECT t.id, t.name, source.b FROM source join t
                            |ON source.pt = t.pt AND source.pt = '2023'
                            |ORDER BY t.id, source.b
                            |""".stripMargin)
      val qe1 = df1.queryExecution
      Assertions.assertFalse(qe1.analyzed.containsPattern(DYNAMIC_PRUNING_SUBQUERY))
      Assertions.assertTrue(qe1.optimizedPlan.containsPattern(DYNAMIC_PRUNING_SUBQUERY))
      Assertions.assertTrue(qe1.sparkPlan.containsPattern(DYNAMIC_PRUNING_SUBQUERY))
      checkAnswer(
        df1,
        Row(1, "a", "x1") :: Row(1, "a", "x2") :: Row(3, "c", "x1") :: Row(3, "c", "x2") :: Nil)

      val df2 = spark.sql("""
                            |SELECT t.*, source.b FROM source join t
                            |ON source.a = t.id AND source.pt = t.pt AND source.a > 3
                            |""".stripMargin)
      val qe2 = df1.queryExecution
      Assertions.assertFalse(qe2.analyzed.containsPattern(DYNAMIC_PRUNING_SUBQUERY))
      Assertions.assertTrue(qe2.optimizedPlan.containsPattern(DYNAMIC_PRUNING_SUBQUERY))
      Assertions.assertTrue(qe2.sparkPlan.containsPattern(DYNAMIC_PRUNING_SUBQUERY))
      checkAnswer(df2, Row(5, "e", "2025", "x5") :: Nil)
    }
  }

  private def getScanBuilder(tableName: String = "T"): ScanBuilder = {
    SparkTable(loadTable(tableName)).newScanBuilder(CaseInsensitiveStringMap.empty())
  }

  def checkFilterExists(sql: String): Boolean = {
    spark.sql(sql).queryExecution.optimizedPlan.exists {
      case Filter(_: Expression, _) => true
      case _ => false
    }
  }

  def checkEqualToFilterExists(sql: String, name: String, value: Literal): Boolean = {
    spark.sql(sql).queryExecution.optimizedPlan.exists {
      case Filter(c: Expression, _) =>
        c.exists {
          case EqualTo(a: AttributeReference, r: Literal) =>
            a.name.equals(name) && r.equals(value)
          case _ => false
        }
      case _ => false
    }
  }
}
