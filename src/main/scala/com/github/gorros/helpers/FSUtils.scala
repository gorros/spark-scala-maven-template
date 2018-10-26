package com.github.gorros.helpers


import java.net.URI

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.sys.process._
import scala.util.Try

object FSUtils {
    def exists(path:String): Boolean = {
        val conf = new Configuration
        val fs = FileSystem.get(URI.create(path), conf)
        val p = new Path(path)
        fs.exists(p)
    }

    def patternMatchesPath(pathPattern: String): Boolean = {
        val conf = new Configuration
        val fs = FileSystem.get(URI.create(pathPattern), conf)
        Try(fs.globStatus(new Path(pathPattern)).length > 0).toOption.getOrElse(false)
    }

    def isEmpty(path: String): Boolean = {
        val conf = new Configuration
        val fs = FileSystem.get(URI.create(path), conf)
        val p = new Path(path)
        if(fs.exists(p)){
            if(fs.getContentSummary(p).getFileCount > 0){
                return false
            }
        }
        return true
    }

    def readFileFully(uri: String): String = {
        val conf = new Configuration
        val fs = FileSystem.get(URI.create(uri), conf)
        Control.using(fs.open(new Path(uri))){
            inputStream => IOUtils.toString(inputStream, "UTF-8")
        }
    }

    def writeToFile(path:String, data:String): Unit = {
        val conf = new Configuration
        val fs = FileSystem.get(URI.create(path), conf)
        Control.using(fs.create(new Path(path))){ f =>
            f.write(data.getBytes)
        }
    }

    def deletePath(path: String): Unit ={
        s"hdfs dfs -rm -r -skipTrash $path".!
    }

    def getFiles(path:String): List[String] = {
        val conf = new Configuration
        val fs = FileSystem.get(URI.create(path), conf)
        fs.globStatus(new Path(path)).filter(_.isFile).map(_.getPath.toString).toList
    }

    def copyFromLocal(src: String, dest: String): Unit = {
        val conf = new Configuration
        val fs = FileSystem.get(conf)
        fs.copyFromLocalFile(new Path(src), new Path(dest))
    }

    def s3distCp(src: String, dest: String): Unit = {
        s"s3-dist-cp --src $src --dest $dest".!
    }
}
