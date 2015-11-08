package parabond.entry

/**
 * @author Ron.Coleman
 */
/**
 * This class contains instrument ids and knows how to value itself
 */
class Portfolio(id : Int, instruments : List[Int]) {
  /** Values the portfolio */
  def value = 0.0
  
  override def toString : String = {
    id + " " + instruments.size + " "  + instruments.foldLeft("") ((xs, x) => xs + " " + x)
  }
}