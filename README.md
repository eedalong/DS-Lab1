# DS-Lab1
 Project for class DS

本次实验尽可能地对着MR地论文实现，其实就对Lab1作业来讲，远远不用写这么多。由于在Lab1中的设定，通信只能由WORKER发起，Master节点只能被动响应，所以有些地方可能设计的比较诡异看起来。如下几点还未实现：
1. TaskID的生成。目前判断两个task是根据任务文件是不是一样进行判断的，但是显然这样比较愚蠢。之前之所以这么做，是因为这样的机制可以使得backup的task更新同一个task的状态数据。
2. Struggle task的识别与backup task的启动。struggle task的识别相对来说比较好解决。相对比较麻烦的就是backup task的标识。如何能区分开backup task与原本的task，同时又能够让他们更新同一个task的数据。其实问题2与问题1是同一个问题。
