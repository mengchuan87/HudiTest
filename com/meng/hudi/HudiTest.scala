package com.meng.hudi

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object HudiTest {
  def main(args: Array[String]): Unit = {
    val sss = SparkSession.builder.appName("hudi")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("hive.metastore.uris", "thrift://ip:port")
      .enableHiveSupport().getOrCreate()

    val sql = "select * from ods.ods_user_event"
    val df: DataFrame = sss.sql(sql)
    

    df.write.format("org.apache.hudi")
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "recordKey")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "update_time")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "date")
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name())
      .option("hoodie.insert.shuffle.parallelism", "10")
      .option("hoodie.upsert.shuffle.parallelism", "10")
      .option(HoodieWriteConfig.TABLE_NAME, "ods.ods_user_event_hudi")
      .mode(SaveMode.Append)
      .save("/user/hudi/lake/ods.db/ods_user_event_hudi")


  }
}
