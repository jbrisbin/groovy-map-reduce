reduceTo = "test.reduce"
reduceAfter = 10
flushAfter = 500

map = { data ->
  println "data: ${data}"
  data[2]
}

reduce = { data ->
  println "testing reduce: ${data.sum()}"
  data.sum()
}