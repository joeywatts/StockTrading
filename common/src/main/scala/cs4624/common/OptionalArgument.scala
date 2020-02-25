package cs4624.common

/**
  * Created by joeywatts on 2/27/17.
  */
case class OptionalArgument[T] (option: Option[T])
object OptionalArgument {
  implicit def any2opt[T](t: T): OptionalArgument[T] = new OptionalArgument(Option(t)) // NOT Some(t)
  implicit def option2opt[T](o: Option[T]): OptionalArgument[T] = new OptionalArgument(o)
  implicit def opt2option[T](o: OptionalArgument[T]): Option[T] = o.option
}
