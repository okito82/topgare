package org
/*

import com.github.sakserv.minicluster.MiniCluster
import com.github.sakserv.minicluster.util.FileUtils
import com.github.sakserv.minicluster.util.WindowsLibsUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.slf4j.Logger
import org.slf4j.LoggerFactory


object HdfsLocalCluster { // Logger
  private val LOG = LoggerFactory.getLogger(classOf[HdfsLocalCluster])

  class Builder {
    private var hdfsNamenodePort = null
    private var hdfsNamenodeHttpPort = null
    private var hdfsTempDir = null
    private var hdfsNumDatanodes = null
    private var hdfsEnablePermissions = false
    private var hdfsFormat = false
    private var hdfsEnableRunningUserAsProxyUser = false
    private var hdfsConfig = null

    def setHdfsNamenodePort(hdfsNameNodePort: Integer): HdfsLocalCluster.Builder = {
      this.hdfsNamenodePort = hdfsNameNodePort
      this
    }

    def setHdfsNamenodeHttpPort(hdfsNameNodeHttpPort: Integer): HdfsLocalCluster.Builder = {
      this.hdfsNamenodeHttpPort = hdfsNameNodeHttpPort
      this
    }

    def setHdfsTempDir(hdfsTempDir: String): HdfsLocalCluster.Builder = {
      this.hdfsTempDir = hdfsTempDir
      this
    }

    def setHdfsNumDatanodes(hdfsNumDatanodes: Integer): HdfsLocalCluster.Builder = {
      this.hdfsNumDatanodes = hdfsNumDatanodes
      this
    }

    def setHdfsEnablePermissions(hdfsEnablePermissions: Boolean): HdfsLocalCluster.Builder = {
      this.hdfsEnablePermissions = hdfsEnablePermissions
      this
    }

    def setHdfsFormat(hdfsFormat: Boolean): HdfsLocalCluster.Builder = {
      this.hdfsFormat = hdfsFormat
      this
    }

    def setHdfsEnableRunningUserAsProxyUser(hdfsEnableRunningUserAsProxyUser: Boolean): HdfsLocalCluster.Builder = {
      this.hdfsEnableRunningUserAsProxyUser = hdfsEnableRunningUserAsProxyUser
      this
    }

    def setHdfsConfig(hdfsConfig: Configuration): HdfsLocalCluster.Builder = {
      this.hdfsConfig = hdfsConfig
      this
    }

    def build: HdfsLocalCluster = {
      val hdfsLocalCluster = new HdfsLocalCluster(this)
      validateObject(hdfsLocalCluster)
      hdfsLocalCluster
    }

    def validateObject(hdfsLocalCluster: HdfsLocalCluster): Unit = {
      if (hdfsLocalCluster.hdfsNamenodePort == null) throw new IllegalArgumentException("ERROR: Missing required config: HDFS Namenode Port")
      if (hdfsLocalCluster.hdfsTempDir == null) throw new IllegalArgumentException("ERROR: Missing required config: HDFS Temp Dir")
      if (hdfsLocalCluster.hdfsNumDatanodes == null) throw new IllegalArgumentException("ERROR: Missing required config: HDFS Num Datanodes")
      if (hdfsLocalCluster.hdfsEnablePermissions == null) throw new IllegalArgumentException("ERROR: Missing required config: HDFS Enable Permissions")
      if (hdfsLocalCluster.hdfsFormat == null) throw new IllegalArgumentException("ERROR: Missing required config: HDFS Format")
      if (hdfsLocalCluster.hdfsConfig == null) throw new IllegalArgumentException("ERROR: Missing required config: HDFS Config")
    }
  }

}

class HdfsLocalCluster private(val builder: HdfsLocalCluster.Builder) extends MiniCluster {
  this.hdfsNamenodePort = builder.hdfsNamenodePort
  this.hdfsNamenodeHttpPort = builder.hdfsNamenodeHttpPort
  this.hdfsTempDir = builder.hdfsTempDir
  this.hdfsNumDatanodes = builder.hdfsNumDatanodes
  this.hdfsEnablePermissions = builder.hdfsEnablePermissions
  this.hdfsFormat = builder.hdfsFormat
  this.hdfsEnableRunningUserAsProxyUser = builder.hdfsEnableRunningUserAsProxyUser
  this.hdfsConfig = builder.hdfsConfig
  var miniDFSCluster: MiniDFSCluster = null
  private var hdfsNamenodePort = null
  private var hdfsNamenodeHttpPort = null
  private var hdfsTempDir = null
  private var hdfsNumDatanodes = null
  private var hdfsEnablePermissions = false
  private var hdfsFormat = false
  private var hdfsEnableRunningUserAsProxyUser = false
  private var hdfsConfig = null

  def getHdfsNamenodePort: Integer = hdfsNamenodePort

  def getHdfsTempDir: String = hdfsTempDir

  def getHdfsNumDatanodes: Integer = hdfsNumDatanodes

  def getHdfsEnablePermissions: Boolean = hdfsEnablePermissions

  def getHdfsFormat: Boolean = hdfsFormat

  def getHdfsEnableRunningUserAsProxyUser: Boolean = hdfsEnableRunningUserAsProxyUser

  def getHdfsConfig: Configuration = hdfsConfig

  @throws[Exception]
  override def start(): Unit = {
    HdfsLocalCluster.LOG.info("HDFS: Starting MiniDfsCluster")
    configure()
    miniDFSCluster = new MiniDFSCluster.Builder(hdfsConfig).nameNodePort(hdfsNamenodePort).nameNodeHttpPort(if (hdfsNamenodeHttpPort == null) 0
    else hdfsNamenodeHttpPort.intValue).numDataNodes(hdfsNumDatanodes).format(hdfsFormat).racks(null).build
  }

  @throws[Exception]
  override def stop(): Unit = {
    stop(true)
  }

  @throws[Exception]
  override def stop(cleanUp: Boolean): Unit = {
    HdfsLocalCluster.LOG.info("HDFS: Stopping MiniDfsCluster")
    miniDFSCluster.shutdown()
    if (cleanUp) cleanUp()
  }

  @throws[Exception]
  override def configure(): Unit = {
    if (null != hdfsEnableRunningUserAsProxyUser && hdfsEnableRunningUserAsProxyUser) {
      hdfsConfig.set("hadoop.proxyuser." + System.getProperty("user.name") + ".hosts", "*")
      hdfsConfig.set("hadoop.proxyuser." + System.getProperty("user.name") + ".groups", "*")
    }
    hdfsConfig.setBoolean("dfs.permissions", hdfsEnablePermissions)
    System.setProperty("test.build.data", hdfsTempDir)
    // Handle Windows
    WindowsLibsUtils.setHadoopHome()
  }

  @throws[Exception]
  override def cleanUp(): Unit = {
    FileUtils.deleteFolder(hdfsTempDir)
  }

  @throws[Exception]
  def getHdfsFileSystemHandle: FileSystem = miniDFSCluster.getFileSystem
}
*/