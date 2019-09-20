package net.qihoo.xsql.mock

import scala.collection.mutable.HashMap

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogTable}
import org.apache.spark.sql.xsql.{CatalogDataSource, DataSourceManager}
import org.apache.spark.sql.xsql.DataSourceType.DataSourceType

class MockDataSource(
  private val name: String,
  private val dsType: DataSourceType,
  override val dsManager: DataSourceManager,
  private val url: String,
  private val username: String,
  private val passwd: String,
  private val version: String)
  extends CatalogDataSource(name, dsType, version, dsManager) {
  def getUrl: String = url
  def getUser: String = username
  def getPwd: String = passwd
}

class MockManager(conf: SparkConf) extends DataSourceManager with Logging {

  override def shortName(): String = "mock"

  override def cacheDatabase(
      isDefault: Boolean,
      dataSourceName: String,
      infos: Map[String, String],
      dataSourcesByName: HashMap[String, CatalogDataSource],
      dataSourceToCatalogDatabase: HashMap[String, HashMap[String, CatalogDatabase]]): Unit = {
    throw new UnsupportedOperationException(s"Get ${shortName()} raw table not supported!")
  }

  override def doGetRawTable(
      db: String,
      originDB: String,
      table: String): Option[CatalogTable] = {
    throw new UnsupportedOperationException(s"Get ${shortName()} raw table not supported!")
  }

  override def tableExists(db: String, table: String): Boolean = {
    throw new UnsupportedOperationException(s"Get ${shortName()} raw table not supported!")
  }
}