package br.com.vprs.utils
import sys.process._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global


object FileUtils {

    def renameFile(path_file: String): Unit = {
        val rename_exec = """hdfs dfs -mv """ + path_file + " "
        val change_name = s"$path_file" + ".proccess"
        (rename_exec.concat(change_name).!!)
    }
}
