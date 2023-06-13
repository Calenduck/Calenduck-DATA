package com.competition.utils

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
object DateUtils {
  def getNow(pattern:String = "yyyy-MM-dd HH:mm:ss"): String ={
    """
      |default pattern = yyyy-MM-dd HH:mm:ss
      |""".stripMargin
    val now = LocalDateTime.now()
    val formatter = DateTimeFormatter.ofPattern(pattern)
    val formatted = now.format(formatter)
    formatted
  }

  def changePattern(date : LocalDate, pattern : String = "yyyyMMdd"): String ={
    val formatter = DateTimeFormatter.ofPattern(pattern)
    val formatted = date.format(formatter)
    formatted
  }


}
