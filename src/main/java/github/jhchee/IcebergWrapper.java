package github.jhchee;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class IcebergWrapper {
    public static SparkSession getSession(String appName) {
        SparkConf conf = new SparkConf();
        conf.setAppName(appName);
        conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
        conf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog");

        // local hive-metastore and minio S3
        conf.set("hive.metastore.uris", "thrift://localhost:9083");
        conf.set("spark.master", "spark://localhost:7077");
        conf.set("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000");
        conf.set("spark.hadoop.fs.s3a.access.key", "accesskey");
        conf.set("spark.hadoop.fs.s3a.secret.key", "secretkey");
        conf.set("spark.hadoop.fs.s3a.path.style.access", "true");
        conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        return SparkSession.builder()
                           .config(conf)
                           .getOrCreate();
    }
}
