6.5830/6.5831 Lab 3: Query Optimization



在本实验中，您将在SimpleDB之上实现一个查询优化器。主要任务包括实现选择性估计框架和基于成本的优化器。您可以自由选择具体实现什么，但我们建议使用类似于课程(第9讲)中讨论的基于成本的塞林格优化器。

本文档的其余部分描述了添加优化器支持所涉及的内容，并提供了如何这样做的基本大纲。

# Getting started

## Implementation hints

在TableStats类中实现允许它使用直方图(为IntHistogram类提供的框架)或您设计的其他形式的统计数据来估计过滤器的选择性和扫描成本的方法。

在JoinOptimizer类中实现允许它估计连接的成本和选择性的方法。

在JoinOptimizer中编写orderJoins方法。该方法必须为一系列连接(可能使用Selinger算法)产生最佳排序，根据在前两步中计算的统计数据。





# 优化器概述

回想一下，基于成本的优化器的主要思想是:

- 使用关于表的统计信息来估计不同查询计划的“成本”。通常，计划的成本与中间连接和选择的基数(由中间连接和选择产生的元组的数量)以及过滤器和连接谓词的选择性有关。
- 使用这些统计信息以最优的方式对连接和选择进行排序，并从几个备选方案中选择连接算法的最佳实现。

在本实验中，您将实现执行这两个功能的代码。

优化器将从simpledb/Parser.java调用。在开始本实验之前，您可能希望复习一下实验2解析器练习。简单地说，如果你有一个目录文件catalog.txt来描述你的表，你可以输入以下命令来运行解析器:

```bash
java -jar dist/simpledb.jar parser catalog.txt
```

当调用Parser时，它将计算所有表的统计信息(使用您提供的统计信息代码)。发出查询时，解析器将查询转换为逻辑计划表示，然后调用查询优化器生成最佳计划。





在开始实现之前，您需要了解SimpleDB优化器的总体结构。解析器和优化器的SimpleDB模块的总体控制流如图1所示。

![lab3-controlflow](lab3-notes.assets/lab3-controlflow.png)

底部的键解释了符号；您将实现带有双边框的组件。类和方法将在接下来的文本中更详细地解释(您可能希望回头参考这张图)，但基本操作如下: 

`Paser.java`在初始化时构造一组表统计信息(存储在statsMap容器中)。然后等待输入一个查询，并对该查询调用parseQuery方法。

`parseQuery`首先构造一个表示已解析查询的`LogicalPlan`。parseQuery然后调用它所构造的LogicalPlan实例上的physicalPlan方法。physicalPlan方法返回一个DBIterator对象，可用于实际运行查询。





# Statistics Estimation

准确估算计划成本是相当棘手的。在本实验中，我们将只关注连接序列和基表访问的代价。我们不会担心访问方法的选择(因为我们只有一个访问方法，表扫描)或额外的操作符的成本(如聚合)。

你只需要考虑这个实验的左深计划(left-deep plan)。请参阅2.3节，了解可能实现的其他“额外”优化器特性的描述，包括处理bushy计划的方法。



## 总体方案成本

我们将按照p=t1 join t2 join…tn，表示左深连接，其中t1是最左边的连接(树中最深的连接)。对于p这样的计划，其成本可以表示为:

```mathematica
scancost(t1) + scancost(t2) + joincost(t1 join t2) + scancost(t3) + joincost((t1 join t2) join t3) + ...
```

其中，scancost(t1)是扫描表t1的I/O开销，joincost(t1,t2)是连接t1到t2的CPU开销。为了使I/O和CPU开销具有可比性，通常使用一个**恒定的缩放因子**，例如:

```mathematica
cost(predicate application) = 1 
cost(pageScan) = SCALING_FACTOR x cost(predicate application)
```

对于本实验，您可以忽略缓存的影响(例如，假设对表的每次访问都会导致扫描的全部成本)——同样，这是您可以在2.3节中作为可选的额外扩展添加到您的实验中的东西。

因此，scancost(t1)就是t1中的页面数x SCALING_FACTOR。



## join 成本

当使用嵌套循环连接时，回想一下两个表t1和t2(其中t1是外层表)之间的连接的代价很简单:

```mathematica
joincost(t1 join t2) = scancost(t1) + ntups(t1) x scancost(t2) //IO cost                        + ntups(t1) x ntups(t2)  //CPU cost
```

这里，ntup (t1)是表t1中元组的数量。





## 过滤器选择性

ntups可以通过扫描基表直接计算出来。

对于有一个或多个选择谓词的表，估计ntups可能比较棘手——这是过滤器选择性估计(filter selectivity estimation)问题。下面是你可能使用的一种方法，基于对表中值的直方图计算:

- 计算表中每个属性的最小值和最大值(扫描一次)。


- 为表中的每个属性构造一个直方图。一个简单的方法是用固定数量的桶NumB，用每个桶表示直方图属性域的固定范围内的记录数。例如，如果一个字段f的范围从1到100，并且有10个桶，那么桶1可能包含1到10之间的记录数量的计数，桶2可能包含11到20之间的记录数量的计数，等等。

- 要估计等式表达式f = const的选择性，计算包含值const的桶。假设桶的宽度(值的范围)是w，高度(元组的数量)是h，表中元组的数量是ntup。然后，假设值均匀分布在整个bucket中，表达式的选择性大致为(h/w) / ntup，因为(h/w)表示值为const的bin中元组的期望数量。

- 要估计范围表达式f > const的选择性，计算const所在的桶b，宽度为w_b，高度为h_b。然后，b包含总元组 b_f = h_b / ntup的一个分数。假设元组在b中均匀分布，则 b中> const的分数b_part为(b_right - const) / w_b，其中b_right是b所在桶的右端点。因此，桶b对谓词贡献(b_f x b_part)选择性。另外，桶b+1…NumB-1贡献了它们所有的选择性(可以使用类似于上面b_f的公式计算)。将所有桶的选择性贡献相加将得到表达式的总体选择性。图2说明了这个过程。

  (**其实我觉得这样算挺怪的，还是按照高中时候学的那种求面积的方法算比较好理解；**)

  ![lab3-hist](lab3-notes.assets/lab3-hist.png)

- 涉及小于表达式的选择性可以执行与大于情况类似的操作，一直查看到0号存储桶。

在接下来的两个练习中，您将编写代码来执行连接和过滤器的选择性估计。



Exercise 1:  IntHistogram.java

您需要实现一些方法来记录表统计信息，以便进行选择性估计。我们已经提供了一个骨架类IntHistogram来做这个。我们的目的是使用上面描述的基于桶的方法来计算直方图，但是只要能够提供合理的选择性估计，您也可以使用其他方法。

我们提供了一个类StringHistogram，它使用IntHistogram来计算String谓词的选择项。如果你想实现一个更好的估计器，你可以修改StringHistogram，不过为了完成这个实验，你不需要这样做。

完成本练习后，您应该能够通过IntHistogramTest单元测试(如果您选择不实现基于直方图的选择性估计，则不需要通过此测试)。如果你用其他方法实现选择性估计，请在你的报告中说明并描述你的方法。





**Exercise 2**

类TableStats包含计算表中元组和页面数量的方法，以及估计谓词对表字段的选择性的方法。我们创建的查询解析器为每个表创建一个TableStats实例，并将这些结构传递给您的查询优化器(您将在后面的练习中用到它)。

你应该在TableStats中完成以下方法和类:

- 实现TableStats构造函数:一旦实现了跟踪统计数据(如直方图)的方法，就应该实现TableStats构造函数，添加代码扫描表(可能多次)以构建所需的统计数据。

- 实现estimateSelectivity(int field, Predicate.Op Op，字段常量): 使用您的统计数据(例如IntHistogram或StringHistogram，取决于字段类型的)，估计表上谓词字段Op常量的选择性。

- 实现estimateScanCost(): 这个方法估计顺序扫描文件的成本，假设读取一个页面的成本是costPerPageIO。您可以假设没有seek（跳过），缓冲池中也没有页。此方法可以使用在构造函数中计算的开销或大小。

- 实现estimateTableCardinality(double selectivityFactor): 如果应用了具有选择性selectivityFactor的谓词，则此方法返回关系中的元组数量。此方法可以使用在构造函数中计算的开销或大小。

您可能希望修改TableStats.java的构造函数，例如，计算字段的直方图，以实现选择性估计。

在完成这些任务之后，您应该能够通过TableStatsTest中的单元测试。















