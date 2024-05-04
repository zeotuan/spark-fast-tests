package com.github.mrpowers.spark.fast.tests

import org.apache.spark.sql.types._
import SparkSessionExt._
import com.github.mrpowers.spark.fast.tests.SchemaComparer.DatasetSchemaMismatch
import org.scalatest.freespec.AnyFreeSpec

object Person {

  def caseInsensitivePersonEquals(some: Person, other: Person): Boolean = {
    some.name.equalsIgnoreCase(other.name) && some.age == other.age
  }
}
case class Person(name: String, age: Int)
case class PrecisePerson(name: String, age: Double)

class DatasetComparerTest extends AnyFreeSpec with DatasetComparer with SparkSessionTestWrapper {

  "checkDatasetEquality" - {
    import spark.implicits._

    "provides a good README example" in {

      val sourceDS = Seq(
        Person("juan", 5),
        Person("bob", 1),
        Person("li", 49),
        Person("alice", 5)
      ).toDS

      val expectedDS = Seq(
        Person("juan", 5),
        Person("frank", 10),
        Person("li", 49),
        Person("lucy", 5)
      ).toDS

      val e = intercept[DatasetContentMismatch] {
        assertSmallDatasetEquality(sourceDS, expectedDS)
      }

    }

    "works with really long columns" in {

      val sourceDS = Seq(
        Person("juanisareallygoodguythatilikealotOK", 5),
        Person("bob", 1),
        Person("li", 49),
        Person("alice", 5)
      ).toDS

      val expectedDS = Seq(
        Person("juanisareallygoodguythatilikealotNOT", 5),
        Person("frank", 10),
        Person("li", 49),
        Person("lucy", 5)
      ).toDS

      val e = intercept[DatasetContentMismatch] {
        assertSmallDatasetEquality(sourceDS, expectedDS)
      }

    }

    "does nothing if the DataFrames have the same schemas and content" in {
      val sourceDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )

      val expectedDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )

      assertSmallDatasetEquality(sourceDF, expectedDF)
      assertLargeDatasetEquality(sourceDF, expectedDF)
    }

    "does nothing if the Datasets have the same schemas and content" in {
      val sourceDS = spark.createDataset[Person](
        Seq(
          Person("Alice", 12),
          Person("Bob", 17)
        )
      )

      val expectedDS = spark.createDataset[Person](
        Seq(
          Person("Alice", 12),
          Person("Bob", 17)
        )
      )

      assertSmallDatasetEquality(sourceDS, expectedDS)
      assertLargeDatasetEquality(sourceDS, expectedDS)
    }

    "works with DataFrames that have ArrayType columns" in {
      val sourceDF = spark.createDF(
        List(
          (1, Array("word1", "blah")),
          (5, Array("hi", "there"))
        ),
        List(
          ("number", IntegerType, true),
          ("words", ArrayType(StringType, true), true)
        )
      )

      val expectedDF = spark.createDF(
        List(
          (1, Array("word1", "blah")),
          (5, Array("hi", "there"))
        ),
        List(
          ("number", IntegerType, true),
          ("words", ArrayType(StringType, true), true)
        )
      )

      assertLargeDatasetEquality(sourceDF, expectedDF)
      assertSmallDatasetEquality(sourceDF, expectedDF)
    }

    "throws an error if the DataFrames have different schemas" in {
      val sourceDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )

      val expectedDF = spark.createDF(
        List(
          (1, "word"),
          (5, "word")
        ),
        List(
          ("number", IntegerType, true),
          ("word", StringType, true)
        )
      )

      val e = intercept[DatasetSchemaMismatch] {
        assertLargeDatasetEquality(sourceDF, expectedDF)
      }
      val e2 = intercept[DatasetSchemaMismatch] {
        assertSmallDatasetEquality(sourceDF, expectedDF)
      }
    }

    "throws an error if the DataFrames content is different" in {
      val sourceDF = Seq(
        (1),
        (5),
        (7),
        (1),
        (1)
      ).toDF("number")

      val expectedDF = Seq(
        (10),
        (5),
        (3),
        (7),
        (1)
      ).toDF("number")

      val e = intercept[DatasetContentMismatch] {
        assertLargeDatasetEquality(sourceDF, expectedDF)
      }
      val e2 = intercept[DatasetContentMismatch] {
        assertSmallDatasetEquality(sourceDF, expectedDF)
      }
    }

    "throws an error if the Dataset content is different" in {
      val sourceDS = spark.createDataset[Person](
        Seq(
          Person("Alice", 12),
          Person("Bob", 17)
        )
      )

      val expectedDS = spark.createDataset[Person](
        Seq(
          Person("Frank", 10),
          Person("Lucy", 5)
        )
      )

      val e = intercept[DatasetContentMismatch] {
        assertLargeDatasetEquality(sourceDS, expectedDS)
      }
      val e2 = intercept[DatasetContentMismatch] {
        assertLargeDatasetEquality(sourceDS, expectedDS)
      }
    }

    "succeeds if custom comparator returns true" in {
      val sourceDS = spark.createDataset[Person](
        Seq(
          Person("bob", 1),
          Person("alice", 5)
        )
      )
      val expectedDS = spark.createDataset[Person](
        Seq(
          Person("Bob", 1),
          Person("Alice", 5)
        )
      )
      assertLargeDatasetEquality(sourceDS, expectedDS, Person.caseInsensitivePersonEquals)
    }

    "fails if custom comparator for returns false" in {
      val sourceDS = spark.createDataset[Person](
        Seq(
          Person("bob", 10),
          Person("alice", 5)
        )
      )
      val expectedDS = spark.createDataset[Person](
        Seq(
          Person("Bob", 1),
          Person("Alice", 5)
        )
      )
      val e = intercept[DatasetContentMismatch] {
        assertLargeDatasetEquality(sourceDS, expectedDS, Person.caseInsensitivePersonEquals)
      }
    }

  }

  "assertLargeDatasetEquality" - {
    import spark.implicits._

    "ignores the nullable flag when making DataFrame comparisons" in {
      val sourceDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, false))
      )

      val expectedDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )

      assertLargeDatasetEquality(sourceDF, expectedDF, ignoreNullable = true)
    }

    "can performed unordered DataFrame comparisons" in {
      val sourceDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )

      val expectedDF = spark.createDF(
        List(
          (5),
          (1)
        ),
        List(("number", IntegerType, true))
      )

      assertLargeDatasetEquality(sourceDF, expectedDF, orderedComparison = false)
    }

    "throws an error for unordered Dataset comparisons that don't match" in {
      val sourceDS = spark.createDataset[Person](
        Seq(
          Person("bob", 1),
          Person("frank", 5)
        )
      )

      val expectedDS = spark.createDataset[Person](
        Seq(
          Person("frank", 5),
          Person("bob", 1),
          Person("sadie", 2)
        )
      )

      val e = intercept[DatasetCountMismatch] {
        assertLargeDatasetEquality(sourceDS, expectedDS, orderedComparison = false)
      }
    }

    "throws an error for unordered DataFrame comparisons that don't match" in {
      val sourceDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )
      val expectedDF = spark.createDF(
        List(
          (5),
          (1),
          (10)
        ),
        List(("number", IntegerType, true))
      )

      val e = intercept[DatasetCountMismatch] {
        assertLargeDatasetEquality(sourceDF, expectedDF, orderedComparison = false)
      }
    }

    "throws an error DataFrames have a different number of rows" in {
      val sourceDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )
      val expectedDF = spark.createDF(
        List(
          (1),
          (5),
          (10)
        ),
        List(("number", IntegerType, true))
      )

      val e = intercept[DatasetCountMismatch] {
        assertLargeDatasetEquality(sourceDF, expectedDF)
      }
    }

    "can performed DataFrame comparisons with unordered column" in {
      val sourceDF = spark.createDF(
        List(
          (1, "word"),
          (5, "word")
        ),
        List(
          ("number", IntegerType, true),
          ("word", StringType, true)
        )
      )
      val expectedDF = spark.createDF(
        List(
          ("word", 1),
          ("word", 5)
        ),
        List(
          ("word", StringType, true),
          ("number", IntegerType, true)
        )
      )
      assertLargeDatasetEquality(sourceDF, expectedDF, ignoreColumnOrder = true)
    }

    "can performed Dataset comparisons with unordered column" in {
      val ds1 = Seq(
        Person("juan", 5),
        Person("bob", 1),
        Person("li", 49),
        Person("alice", 5)
      ).toDS

      val ds2 = Seq(
        Person("juan", 5),
        Person("bob", 1),
        Person("li", 49),
        Person("alice", 5)
      ).toDS.select("age", "name").as(ds1.encoder)

      assertLargeDatasetEquality(ds1, ds2, ignoreColumnOrder = true)
      assertLargeDatasetEquality(ds2, ds1, ignoreColumnOrder = true)
    }
  }

  "assertSmallDatasetEquality" - {
    import spark.implicits._

    "ignores the nullable flag when making DataFrame comparisons" in {
      val sourceDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, false))
      )

      val expectedDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )

      assertSmallDatasetEquality(sourceDF, expectedDF, ignoreNullable = true)
    }

    "can performed unordered DataFrame comparisons" in {
      val sourceDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )
      val expectedDF = spark.createDF(
        List(
          (5),
          (1)
        ),
        List(("number", IntegerType, true))
      )
      assertSmallDatasetEquality(sourceDF, expectedDF, orderedComparison = false)
    }

    "can performed unordered Dataset comparisons" in {
      val sourceDS = spark.createDataset[Person](
        Seq(
          Person("bob", 1),
          Person("alice", 5)
        )
      )
      val expectedDS = spark.createDataset[Person](
        Seq(
          Person("alice", 5),
          Person("bob", 1)
        )
      )
      assertSmallDatasetEquality(sourceDS, expectedDS, orderedComparison = false)
    }

    "throws an error for unordered Dataset comparisons that don't match" in {
      val sourceDS = spark.createDataset[Person](
        Seq(
          Person("bob", 1),
          Person("frank", 5)
        )
      )
      val expectedDS = spark.createDataset[Person](
        Seq(
          Person("frank", 5),
          Person("bob", 1),
          Person("sadie", 2)
        )
      )
      val e = intercept[DatasetContentMismatch] {
        assertSmallDatasetEquality(sourceDS, expectedDS, orderedComparison = false)
      }
    }

    "throws an error for unordered DataFrame comparisons that don't match" in {
      val sourceDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )
      val expectedDF = spark.createDF(
        List(
          (5),
          (1),
          (10)
        ),
        List(("number", IntegerType, true))
      )
      val e = intercept[DatasetContentMismatch] {
        assertSmallDatasetEquality(sourceDF, expectedDF, orderedComparison = false)
      }
    }

    "throws an error DataFrames have a different number of rows" in {
      val sourceDF = spark.createDF(
        List(
          (1),
          (5)
        ),
        List(("number", IntegerType, true))
      )
      val expectedDF = spark.createDF(
        List(
          (1),
          (5),
          (10)
        ),
        List(("number", IntegerType, true))
      )
      val e = intercept[DatasetContentMismatch] {
        assertSmallDatasetEquality(sourceDF, expectedDF)
      }
    }

    "can performed DataFrame comparisons with unordered column" in {
      val sourceDF = spark.createDF(
        List(
          (1, "word"),
          (5, "word")
        ),
        List(
          ("number", IntegerType, true),
          ("word", StringType, true)
        )
      )
      val expectedDF = spark.createDF(
        List(
          ("word", 1),
          ("word", 5)
        ),
        List(
          ("word", StringType, true),
          ("number", IntegerType, true)
        )
      )
      assertSmallDatasetEquality(sourceDF, expectedDF, ignoreColumnOrder = true)
    }

    "can performed Dataset comparisons with unordered column" in {
      val ds1 = Seq(
        Person("juan", 5),
        Person("bob", 1),
        Person("li", 49),
        Person("alice", 5)
      ).toDS

      val ds2 = Seq(
        Person("juan", 5),
        Person("bob", 1),
        Person("li", 49),
        Person("alice", 5)
      ).toDS.select("age", "name").as(ds1.encoder)

      assertSmallDatasetEquality(ds1, ds2, ignoreColumnOrder = true)
      assertSmallDatasetEquality(ds2, ds1, ignoreColumnOrder = true)
    }

  }

  "defaultSortDataset" - {

    "sorts a DataFrame by the column names in alphabetical order" in {
      val sourceDF = spark.createDF(
        List(
          (5, "bob"),
          (1, "phil"),
          (5, "anne")
        ),
        List(
          ("fun_level", IntegerType, true),
          ("name", StringType, true)
        )
      )
      val actualDF = defaultSortDataset(sourceDF)
      val expectedDF = spark.createDF(
        List(
          (1, "phil"),
          (5, "anne"),
          (5, "bob")
        ),
        List(
          ("fun_level", IntegerType, true),
          ("name", StringType, true)
        )
      )
      assertSmallDatasetEquality(actualDF, expectedDF)
    }

  }
}
