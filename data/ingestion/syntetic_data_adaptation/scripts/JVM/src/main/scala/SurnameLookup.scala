


object SurnameLookup{
    def lookup(
        ticket:Long,
        thresholds:Array[Long],
        surnames:Array[String]
        ): String = {

        var low:Int = 0
        var high:Int = (thresholds.length - 1)

        while (low < high){

            val middle:Int = low + (high-low)/2

            if (thresholds(middle) <= ticket){
                low = middle + 1
            }
            else{
                high = middle
            }
        }
        surnames(low)
   }
}