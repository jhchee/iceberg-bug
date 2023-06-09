package github.jhchee;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class ScdExample1 extends IcebergWrapper {
    public static void main(String[] args) {
        SparkSession spark = getSession("ScdExample1");

        // create if not exists
        spark.sql("CREATE TABLE IF NOT EXISTS default.user_scd ( " +
                "user_id string, " +
                "first_name string, " +
                "last_name string, " +
                "valid_from timestamp, " +
                "valid_to timestamp, " +
                "is_current boolean, " +
                "checksum string " +
                ") " +
                "USING iceberg " +
                "TBLPROPERTIES ('format-version'='2')"+
                "");

        List<Row> rows = Arrays.asList(
                RowFactory.create("9612e6e0-2a7f-4198-a617-ad5b5f8e8307", "Mike", "Smith"),
                RowFactory.create(UUID.randomUUID().toString(), "Bob", "Clark"),
                RowFactory.create(UUID.randomUUID().toString(), null, null)
        );
        StructType schema = new StructType().add("user_id", DataTypes.StringType, false)
                                            .add("first_name", DataTypes.StringType, true)
                                            .add("last_name", DataTypes.StringType, true);


        Dataset<Row> updates = spark.createDataFrame(rows, schema);
        // generates the scd columns e.g. valid_from, valid_to, is_current and checksum
        updates = updates.selectExpr(
                "*",
                "current_timestamp() AS valid_from",
                "null AS valid_to",
                "true AS is_current",
                "md5(concat(first_name, last_name)) AS checksum"
        );
        Dataset<Row> userScdTable = spark.table("default.user_scd");

        // Rows to INSERT new users profile
        Dataset<Row> newUserToInsert = updates.as("updates").join(userScdTable.as("user_scd"), "user_id")
                                              .where("user_scd.is_current is true AND user_scd.checksum != updates.checksum");
        // Stage the update by unioning two sets of rows
        // 1. Rows that will be inserted in the whenNotMatched clause
        // 2. Rows that will either update the existing users or insert the new users
        Dataset<Row> stagedUpdates = newUserToInsert.selectExpr("NULL as merge_key", "updates.*")
                                                    .union(updates.selectExpr("user_id as merge_key", "*"));
        stagedUpdates.createOrReplaceTempView("staged_updates");
        stagedUpdates.show(); // print the followings
//      +--------------------+--------------------+----------+---------+--------------------+--------+----------+--------------------+
//      |           merge_key|             user_id|first_name|last_name|          valid_from|valid_to|is_current|            checksum|
//      +--------------------+--------------------+----------+---------+--------------------+--------+----------+--------------------+
//      |9612e6e0-2a7f-419...|9612e6e0-2a7f-419...|      Mike|    Smith|2023-06-04 13:06:...|    null|      true|a9b326df4e6da61c5...|
//      |679226b6-a401-4c9...|679226b6-a401-4c9...|       Bob|    Clark|2023-06-04 13:06:...|    null|      true|75c62a555f786da13...|
//      |4ede8f3e-f5f9-4a5...|4ede8f3e-f5f9-4a5...|      null|     null|2023-06-04 13:06:...|    null|      true|                null|
//      +--------------------+--------------------+----------+---------+--------------------+--------+----------+--------------------+

        // ERROR thrown
        spark.sql(
                "MERGE INTO default.user_scd AS target " +
                "USING (SELECT * FROM staged_updates) " +
                "ON target.user_id = staged_updates.merge_key " +
                "WHEN MATCHED AND staged_updates.checksum != target.checksum AND target.is_current IS true " +
                "THEN UPDATE SET valid_to = staged_updates.valid_from, is_current = false " +
                "WHEN NOT MATCHED THEN INSERT * "
        );

    }
}
