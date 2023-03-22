val numberList = List(1, 2, 3, 4, 5)
val sum = numberList.reduce( (x: Int, y: Int) =>{ println(x); x + y} )
println(sum)