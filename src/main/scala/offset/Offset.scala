package offset

import java.util.Date

/**
  * Created by prayagupd
  * on 1/19/17.
  */

case class PartitionOffset(partition: Long, offset: Offset)
case class Offset(offset: Long, timestamp: Date)
