# DS-Lab1
 Project for class DS

本次实验尽可能地对着MR地论文实现，其实就对Lab1作业来讲，远远不用写这么多。由于在Lab1中的设定，通信只能由WORKER发起，Master节点只能被动响应，所以有些地方可能设计的比较诡异看起来。如下几点还未实现：
1. TaskID的生成。目前判断两个task是根据任务文件是不是一样进行判断的，但是显然这样比较愚蠢。之前之所以这么做，是因为这样的机制可以使得backup的task更新同一个task的状态数据。
2. Struggle task的识别与backup task的启动。struggle task的识别相对来说比较好解决。相对比较麻烦的就是backup task的标识。如何能区分开backup task与原本的task，同时又能够让他们更新同一个task的数据。其实问题2与问题1是同一个问题。
3. 垃圾清理问题。垃圾清理主要是指中间文件清理。第一版实现的时候垃圾清理实际上是放在master节点来做。在这个实验中，这么做从结果上看没有问题。但是这显然与实际不符合。所以后来把垃圾清理放在了worker端。但是目前面临的问题就是，go没有try catch机制，如果程序被kill，或者某个步骤出错导致程序运行失败，在只能此修改worker的情况下，目前我还没找到一个优雅的方法进行垃圾清除。这样导致的结果是测试crash的时候，会有一部分中间文件残留下来。

4. 任务进度报告特性没有实现。因为任务实在太小了。正常来说，Map阶段的进度报告是由文件内容读取比例算出来的。在这个实验中map的task_file是直接一下读完的。reduce的任务报告倒是可以做，按照一个个文件算就行。但是在这个任务中意义不大。所以就没写了

================================更新=====================================================================================================

5. 读了一下Hadoop的源码。发现解决1、2问题的好办法。实际上在我原来的实现中，之所以1、2变得困难是因为Task本身和一个executive unit混为一谈了。Hadoop用了AttemptTask来解决这个问题。这就意味着一个task可以对应多个attemptTask。很好的解决了1、2问题

6. 完成了任务进度报告。但是并不是从worker端统计，是从master端统计。

垃圾清理问题目前还未解决。要是能捕获异常，这就不事儿了。

7. 垃圾清理实际上是一定要借助Master节点的。即Master告诉机器上的Worker应该清理哪些垃圾。所以此时应该再把Node和Worker分开。Node里面保存所有中间的文件信息，如果一个worker没来得及清理就crash了，Master应该把该垃圾清理任务交给同一个Node上的其他Worker。
