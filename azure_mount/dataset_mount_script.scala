// Databricks notebook source
def getAzureRegion():String = {
  import com.databricks.backend.common.util.Project
  import com.databricks.conf.trusted.ProjectConf
  import com.databricks.backend.daemon.driver.DriverConf

  new DriverConf(ProjectConf.loadLocalConfig(Project.Driver)).region
}

// COMMAND ----------

def mountFailed(msg: String): Unit = {
  println(msg)
}

def retryMount(source: String, mountPoint: String): Unit = {
  try {
    dbutils.fs.mount(source, mountPoint)
  } catch {
    case e: Exception => mountFailed(s"Unable to mount $mountPoint: ${e.getMessage}")
  }
}

def mount(source: String, extraConfigs: Map[String, String], mountPoint: String): Unit = {
  try {
    dbutils.fs.mount(source, mountPoint, extraConfigs=extraConfigs)
  } catch {
    case ioe: java.lang.IllegalArgumentException => retryMount(source, mountPoint)
    case e: Exception => mountFailed(s"Unable to mount $mountPoint: ${e.getMessage}")
  }
}

// COMMAND ----------

def mountSource(mountDir: String, source: String, extraConfigs: Map[String, String]): String = {
  val mntSource = source

  if (dbutils.fs.mounts().map(_.mountPoint).contains(mountDir)) {
    val mount = dbutils.fs.mounts().filter(_.mountPoint == mountDir).head
    if (mount.source == mntSource) {
      return s"""Datasets already mounted to <b>$mountDir</b> from <b>$mntSource</b>"""
    }
    else {
      return "Mount is invalid"
    }
  }
  else {
    println(s"""Mounting datasets to $mountDir from $mntSource""")
    mount(source, extraConfigs, mountDir)
    return s"""Mounted datasets to <b>$mountDir</b> from <b>$mntSource<b>"""
  }
}

// COMMAND ----------

def initAzureDataSource(azureRegion:String):(String,Map[String,String]) = {
  val account = "<storage account>"
  val container = "<container name>"
  val source = s"wasbs://$container@$account.blob.core.windows.net/"
  val sasKey = "<sas Key>"
  val config = Map(
    s"fs.azure.sas.$container.$account.blob.core.windows.net" -> sasKey)
  (source, config)
}

// COMMAND ----------

def autoMount(mountDir: String = "/mnt/databricks-stream-eh"): Unit = {
    val azureRegion = getAzureRegion()
    println(s"Region: $azureRegion")
    val (source, extraConfigs) = initAzureDataSource(azureRegion)  
  val resultMsg = mountSource( mountDir, source, extraConfigs)
  displayHTML(resultMsg)
}

// COMMAND ----------

autoMount()
