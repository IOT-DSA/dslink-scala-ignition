# dslink-scala-ignition

![alt tag](https://travis-ci.org/IOT-DSA/dslink-scala-ignition.svg?branch=master)

Scala-based DSLink for running dataflows based on [Reactive Streams](http://www.reactive-streams.org) architecture. 

## Overview
 - Uses ReactiveX paradigm for manipulating data streams.
 - Implements the majority of [RxScala](http://reactivex.io/rxscala/) operators for stream operations.
 - Easily extensible to implement your own building blocks.
 - Implements an adapter for Spark Ignition, allowing you to embed [Apache Spark](http://spark.apache.org/) operations.
 - Supports input and output blocks for integration into DSA environment.
 - Supports scripting for implementing data processing logic. Currently supports the following dialects:
   - MVEL
   - Scala
   - Java
   - Groovy
   - XPath
   - JsonPath
 - Supports the backpressure operators for smooth scaling (*work in progress*).
 - Can be used both as an embedded library and dataflow editor inside DGLux.
 - Provides automatic conversion between all basic DSA datatypes and Spark DataFrames.
 - Can be easily extended to integrate other data processing engines, such as Apache Flink or Kafka Connect.

## Essential Information

### License

The license is Apache 2.0, see [LICENSE](https://github.com/IOT-DSA/dslink-scala-ignition/blob/documentation/LICENSE).

### Binary Releases

You can find published releases on Maven Central.

    <dependency>
        <groupId>org.iot-dsa</groupId>
        <artifactId>dslink-scala-ignition_2.11</artifactId>
        <version>0.1.0</version>
    </dependency>

sbt dependency:

    libraryDependencies += "org.iot-dsa" %% "dslink-scala-ignition" % "0.1.0"
    
Gradle dependency:

	compile group: 'org.iot-dsa', name: 'dslink-scala-ignition_2.11', version: '0.1.0'
	
### API Docs

The complete Scaladoc bundle is available online 
at [github.io](http://iot-dsa.github.io/dslink-scala-ignition/latest/api/).

### Installation in DGLux

1. Connect to DGLux platform by navigating to http://localhost:8080 in your browser.
2. Switch to **Data** panel on the left, expand **sys** node and right click on **links**. 
4. Select *Install Link* command in the context menu and choose **Ignition**.
5. Choose any name you want and click *Invoke*. The Ignition DSLink will be installed to your platform.

## Using Ignition

### Ignition RX architecture

Ignition RX is a collection of building blocks which allow you to build Reactive Streams 
dataflow. Each block has one *output* and any number of *inputs* and *attributes*. Technically,
both inputs and attributes perform similar function -- they serve as ports where data events 
from other blocks can enter. However, they are conceptually different: an attribute port can 
be also initialized to a static value; also, a new values entering an attribute port will 
"reset" the block.

For example, consider [TakeByCount](http://iot-dsa.github.io/dslink-scala-ignition/latest/api/#org.dsa.iot.rx.core.TakeByCount)
block, which has one input and one attribute named `count`. Its purpose is to pass to its output 
only N first items arriving to its input. Each time `count` port receives new value (either set
statically or from another block's output connected to it), the block will reset its counter and
pass the next `count` values to its output.

Another example, [Interval](http://iot-dsa.github.io/dslink-scala-ignition/latest/api/#org.dsa.iot.rx.core.Interval)
block that has no inputs and generates sequential numbers 0, 1, 2,... on its output at equal time 
intervals. Each time its `period` port's value is changed, the block will restart the generated
sequence from 0 with the new time interval.

### First Ignition RX application

From the programming standpoint, the output of each block represents an [Observable](http://reactivex.io/documentation/observable.html)
that your application can subscribe to.

Below is a trivial example of how one can create Ignition RX workflows:

```scala
import scala.concurrent.duration._
import org.dsa.iot.rx._
import org.dsa.iot.rx.core._

val i1 = Interval(100 milliseconds, 50 milliseconds)

val cmb = CombineLatest3[Long, String, Boolean]
cmb.output subscribe (println(_))

cmb.source1 <~ i1
cmb.source2 <~ "hello"
cmb.source3 <~ true
i1.reset
Thread.sleep(200)

i1.period <~ (50 milliseconds)
cmb.source2 <~ "world"
i1.reset
Thread.sleep(200)

cmb.source3 <~ false
cmb.reset
Thread.sleep(100)

i1.shutdown
cmb.shutdown
```

Depending on your logging configuration, you may observe the program's output similar to that 
on the listing below:

```
  15:45:25.390 INFO  Interval486.initial set to 50 milliseconds
  15:45:25.391 INFO  Interval486.period set to 100 milliseconds
  15:45:25.391 INFO  CombineLatest3621.source1 bound to Interval486.output
  15:45:25.391 INFO  CombineLatest3621.source2 set to hello
  15:45:25.391 INFO  CombineLatest3621.source3 set to true
  (0,hello,true)
  (1,hello,true)
  15:45:25.594 INFO  Interval486.period set to 50 milliseconds
  15:45:25.594 INFO  CombineLatest3621.source2 set to world
  (0,world,true)
  (1,world,true)
  (2,world,true)
  15:45:25.801 INFO  CombineLatest3621.source3 set to false
  (3,world,false)
  (4,world,false)
  (5,world,false)
```

First, two blocks are created: *Interval*, which generates numbers 0, 1, 2,... etc every 100
milliseconds with the initial delay of 50 milliseconds, and *CombineLatest3* - a block that has
three inputs and generates a tuple of 3 elements each time the value of any of its inputs 
changes. 

![CombineLatest](https://blogs.endjin.com/wp-content/uploads/2014/05/combine-latest.png)

We subscribe to the output of the second block to print out each such tuple as it 
emerges on its output.

Then, we connect the first input of CombineLatest to the output of Interval (i.e. it is 
going receive numbers 1, 2, 3, ... once the workflow has been started). We set the second
input to a static value "hello", and the third - to the static value `true`. Note how the type
of each input is consistent with the type parameters of the block: 
`CombineLatest3[Long, String, Boolean]`, otherwise the compiler would generate an error.

We then call `reset()` method on the Interval, which restarts the sequence. For each produced
value, the CombineLatest block emits a corresponding tuple on its output. The delay of 200
milliseconds allows a few items to be generated, then we change the Interval's period and
reset the CombineLatest's second input to another static value - "world". After resetting the
Interval once again, its sequence is restarted and a few more items are produced. Finally,
we change the third input of CombineLatest to `false` and now restart only the second block,
without touching the interval. As you can see, the number sequence is not restarted, but the
third value of each tuple is now `false`.


