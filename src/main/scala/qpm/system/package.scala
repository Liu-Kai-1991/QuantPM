package qpm

package object system {
  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]):Traversable[(X,Y)] = for { x <- xs; y <- ys } yield (x, y)
  }
}
