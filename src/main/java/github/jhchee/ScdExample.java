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

public class ScdExample extends IcebergWrapper {
    public static void main(String[] args) {
        SparkSession spark = getSession("ScdExample");

        spark.sql("CREATE TABLE default.user_scd ( " +
                "user_id string, " +
                "first_name string, " +
                "last_name string, " +
                "valid_from timestamp, " +
                "valid_to timestamp, " +
                "is_current boolean, " +
                "checksum string " +
                ") " +
                "USING iceberg");

        List<Row> rows = Arrays.asList(
                RowFactory.create("9612e6e0-2a7f-4198-a617-ad5b5f8e8307", "Mike", "Smith"),
                RowFactory.create(UUID.randomUUID().toString(), "Bob", "Clark"),
                RowFactory.create(UUID.randomUUID().toString(), null, null)
        );
        StructType schema = new StructType().add("user_id", DataTypes.StringType, false)
                                            .add("first_name", DataTypes.StringType, true)
                                            .add("last_name", DataTypes.StringType, true);

        Dataset<Row> updates = spark.createDataFrame(rows, schema);
        updates = updates.selectExpr(
                "*",
                "current_timestamp() AS valid_from",
                "null AS valid_to",
                "true AS is_current",
                "md5(concat(first_name, last_name)) AS checksum"
        );
        Dataset<Row> userScd = spark.table("default.user_scd");

        Dataset<Row> newUserToInsert = updates.as("updates").join(userScd.as("user_scd"), "user_id")
                                              .where("user_scd.is_current is true AND user_scd.checksum != updates.checksum");
        Dataset<Row> stagedUpdates = newUserToInsert.selectExpr("NULL as merge_key", "updates.*")
                                                    .union(updates.selectExpr("user_id as merge_key", "*"));
        stagedUpdates.createOrReplaceTempView("staged_updates");
        stagedUpdates.show();

        spark.sql(
                "MERGE INTO default.user_scd AS target " +
                        "USING (SELECT * FROM staged_updates) " +
                        "ON target.user_id = staged_updates.merge_key " +
                        "WHEN MATCHED AND staged_updates.checksum != target.checksum AND target.is_current IS true " +
                        "THEN UPDATE SET valid_to = staged_updates.valid_from, is_current = false " +
                        "WHEN NOT MATCHED THEN INSERT * "
        );

        spark.sql("DROP TABLE IF EXISTS default.user_scd");
    }
}
