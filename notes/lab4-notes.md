在本实验中，您将在SimpleDB中实现一个简单的基于锁的事务系统。您需要在代码中适当的位置添加锁和解锁调用，还需要在代码中跟踪每个事务所持有的锁，并根据需要将锁授予事务。

本文档的其余部分描述了添加事务支持所涉及的内容，并提供了如何将此支持添加到数据库的基本大纲。

与前面的实验一样，我们建议您尽早开始。

锁定和事务调试起来相当棘手!

# Getting started

你应该从你提交给实验室3的代码开始(如果你没有提交实验室3的代码，或者你的解决方案不能正常工作，请联系我们讨论选项)。此外，我们为这个实验室提供了额外的测试用例，这些用例不在您收到的原始代码分发中。我们重申，我们提供的单元测试是为了帮助指导您的实现，但它们并不打算是全面的或建立正确性。



# 事务、锁定和并发控制

在开始之前，您应该确保了解什么是事务，以及严格的两阶段锁定(用于确保事务的隔离性和原子性)是如何工作的。

在本节的其余部分中，我们将简要概述这些概念，并讨论它们与SimpleDB的关系。



## 事务

事务是一组以原子方式执行的数据库操作(例如插入、删除和读取);也就是说，要么所有的操作都完成了，要么没有一个完成。对于数据库用户来说，这些操作不是作为单个不可分割的操作的一部分完成的，（逻辑上作为单个不可分割的操作）对于用户是不可见的。



## ACID特性

为了帮助您理解SimpleDB中的事务管理是如何工作的，我们简要回顾一下它是如何确保满足ACID属性的:

- 原子性: 严格的两阶段锁定和仔细的缓冲区管理确保了原子性。

- 一致性: 由于原子性，数据库是事务一致的。其他一致性问题(例如，键约束)在SimpleDB中没有解决。

- 隔离: 严格的两相锁提供隔离。

- 持久性: FORCE类型的缓冲区管理策略确保持久性(参见下面的第2.3节)。





## 恢复和缓冲区管理

为了简化您的工作，我们建议您实现NO STEAL/FORCE缓冲区管理策略。

正如我们在课堂上讨论的，这意味着:

- 如果脏(更新的)页被未提交的事务锁定(这是NO STEAL)，则不应该从缓冲池中驱逐它们。
- 在事务提交时，您应该强制脏页到磁盘(例如，将页写出来)(这是force)。

为了进一步简化您的工作，您可以假设SimpleDB在处理transactionComplete命令时不会崩溃。注意，这三点意味着您不需要在这个实验中实现基于日志的恢复，因为您永远不需要撤销任何工作(您永远不需要清除脏页)，也永远不需要重做任何工作(您在提交时强制更新，并且不会在提交处理期间崩溃)。





## 授予锁

您将需要向SimpleDB(例如，在BufferPool中)添加调用，以允许调用方代表特定事务请求或释放特定对象上的(共享或独占)锁。

我们建议**锁定页面粒度**; 为了测试的简单性，请不要实现表级锁定(即使有可能)。本文档的其余部分和单元测试假设页面级锁定。

您将需要**创建数据结构来跟踪每个事务持有哪些锁**，并检查在请求事务时是否应该将锁授予该事务。

您将需要实现共享锁和独占锁（排他锁）; 回顾这些工作如下:

- 在事务读取对象之前，它必须在该对象上具有共享锁。

- 在事务可以写入对象之前，它必须对该对象具有排他锁。

- 多个事务可以在一个对象上拥有一个共享锁。

- 一个对象上只能有一个事务具有排他锁。

- 如果事务t是在对象o上持有共享锁的唯一事务，t可以将其在对象o上的锁升级为排他锁。

如果一个事务请求一个不能立即授予的锁，您的代码应该阻塞，等待该锁变得可用(即，锁由运行在不同线程中的另一个事务释放)。**注意锁实现中的竞争条件——考虑对锁的并发调用可能会如何影响行为**。你一定想读一下Thread Synchronization in Java





**Exercise 1**

编写在BufferPool中获取和释放锁的方法。假设您正在使用页级锁，您将需要完成以下操作: 

- 修改`getPage()`以阻塞并在返回页之前获得所需的锁。
- 实现`unsaferreleepage()`。此方法主要用于测试和事务结束时。

- 实现`holdsLock()`，以便练习2中的逻辑可以确定一个页面是否已经被事务锁定。


您可能会发现定义一个负责维护事务和锁状态的LockManager类很有帮助，但取决于您。

在代码通过LockingTest中的单元测试之前，您可能需要实现下一个练习。







```java
package simpledb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Administrator on 2020/5/1 0001.
 */
public class TxLockManager {

//    private TxLockManager instance;
    long txFinishTimeoutMills = 1000;
    long waitTimeoutMills = 20;
    long randomDelayTime = 200;
//    long start;

    private ConcurrentHashMap<PageId, List<TransactionId>> page2TxReadLockMap;
    private ConcurrentHashMap<PageId, List<TransactionId>> page2TxWriteLockMap;

    private ConcurrentHashMap<TransactionId, List<PageId>> tx2PageReadLockMap;
    private ConcurrentHashMap<TransactionId, List<PageId>> tx2PageWriteLockMap;

    public ConcurrentHashMap<PageId, List<TransactionId>> getPage2TxReadLockMap() {
        return page2TxReadLockMap;
    }

    public void setPage2TxReadLockMap(ConcurrentHashMap<PageId, List<TransactionId>> page2TxReadLockMap) {
        this.page2TxReadLockMap = page2TxReadLockMap;
    }

    public ConcurrentHashMap<PageId, List<TransactionId>> getPage2TxWriteLockMap() {
        return page2TxWriteLockMap;
    }

    public void setPage2TxWriteLockMap(ConcurrentHashMap<PageId, List<TransactionId>> page2TxWriteLockMap) {
        this.page2TxWriteLockMap = page2TxWriteLockMap;
    }

    public ConcurrentHashMap<TransactionId, List<PageId>> getTx2PageReadLockMap() {
        return tx2PageReadLockMap;
    }

    public void setTx2PageReadLockMap(ConcurrentHashMap<TransactionId, List<PageId>> tx2PageReadLockMap) {
        this.tx2PageReadLockMap = tx2PageReadLockMap;
    }

    public ConcurrentHashMap<TransactionId, List<PageId>> getTx2PageWriteLockMap() {
        return tx2PageWriteLockMap;
    }

    public void setTx2PageWriteLockMap(ConcurrentHashMap<TransactionId, List<PageId>> tx2PageWriteLockMap) {
        this.tx2PageWriteLockMap = tx2PageWriteLockMap;
    }


    public TxLockManager () {
        page2TxReadLockMap = new ConcurrentHashMap<>();
        page2TxWriteLockMap = new ConcurrentHashMap<>();

        tx2PageReadLockMap = new ConcurrentHashMap<>();
        tx2PageWriteLockMap = new ConcurrentHashMap<>();

    }


    // tid want lock pid with perm
    public boolean acquireLock(TransactionId tid, PageId pid, Permissions perm, long startAt, int tryTimes) throws TransactionAbortedException {
        try {
            // if already has this lock
            if (perm.equals(Permissions.READ_ONLY) && page2TxReadLockMap.get(pid) != null && page2TxReadLockMap.get(pid).contains(tid) ||
                    perm.equals(Permissions.READ_WRITE) && page2TxWriteLockMap.get(pid) != null && page2TxWriteLockMap.get(pid).contains(tid) ||
                    perm.equals(Permissions.READ_ONLY) && page2TxWriteLockMap.get(pid) != null && page2TxWriteLockMap.get(pid).contains(tid)) {
                // already has READ or WRITE Lock
                // get the lock and return
//                System.out.println("tid = "+tid.getId()+" get "+perm+" Lock :"+pid.getPageNumber());
                return true;
            }

            // try get READ lock
            if (perm.equals(Permissions.READ_ONLY)) {
                // if a WRITE Lock exist, can't get read lock
                if (page2TxWriteLockMap.get(pid) != null && !page2TxWriteLockMap.get(pid).isEmpty()) {
//                    System.out.println("tid = "+tid.getId()+" try get pid = "+pid.getPageNumber()+" READ Lock." +" But some WRITE Lock at pid = "+pid.getPageNumber());

                    synchronized (pid) {
                        waitOrAbort ( pid, tid, perm, startAt);
                        while (!acquireLock(tid, pid, perm, startAt, tryTimes+1)) {
                            waitOrAbort ( pid, tid, perm, startAt);
                        }
                    }

//                    System.out.println("tid = "+tid.getId()+" get "+perm+" Lock :"+pid.getPageNumber());
                    return true;
                }

                // No WRITE exist
                // So don't care whether has another tid's READ Lock, just add a READ Lock
                List<TransactionId> page2TxReadLockList = page2TxReadLockMap.get(pid);

                addLockIntoPage2TxMap( page2TxReadLockList,  tid,  pid,  page2TxReadLockMap);

                // get the lock
//                System.out.println("tid = "+tid.getId()+" get "+perm+" Lock :"+pid.getPageNumber());
                return true;
            }


            // try get WRITE lock
            if (perm.equals(Permissions.READ_WRITE)) {
                List<TransactionId> page2TxWriteLockList = page2TxWriteLockMap.get(pid);

                // only self has pid's READ lock, no one has WRITE Lock
                if (page2TxWriteLockMap.get(pid)==null ||
                        (page2TxWriteLockMap.get(pid) != null && page2TxWriteLockMap.get(pid).isEmpty())) {
                    if (page2TxReadLockMap.get(pid) != null && page2TxReadLockMap.get(pid).contains(tid) && page2TxReadLockMap.get(pid).size() == 1) {
                        // remove READ Lock, add WRITE Lock
                        page2TxReadLockMap.get(pid).remove(tid);

                        addLockIntoPage2TxMap( page2TxWriteLockList,  tid,  pid,  page2TxWriteLockMap);

//                        System.out.println("tid = "+tid.getId()+" get "+perm+" Lock :"+pid.getPageNumber());
                        return true;
                    }
                }

                // whatever another tid(not this tid) take Read or WRITE Lock for this pid, can't get pid WRITE lock
                if (page2TxWriteLockMap.get(pid) != null && !page2TxWriteLockMap.get(pid).isEmpty() ||
                        page2TxReadLockMap.get(pid) != null && !page2TxReadLockMap.get(pid).isEmpty()) {
//                    System.out.println("tid = "+tid.getId()+" try get pid = "+pid.getPageNumber()+" WRITE Lock."+" But some other Lock at pid = "+pid.getPageNumber());

                    synchronized (pid) {
                        waitOrAbort ( pid, tid, perm, startAt);
                        while (!acquireLock(tid, pid, perm, startAt, tryTimes+1)) {
                            waitOrAbort ( pid, tid, perm, startAt);
                        }
                    }

//                    System.out.println("tid = "+tid.getId()+" get "+perm+" Lock :"+pid.getPageNumber());
                    return true;
                }

                // get the lock as no other WRITE or READ Lock
                addLockIntoPage2TxMap( page2TxWriteLockList,  tid,  pid,  page2TxWriteLockMap);

//                System.out.println("tid = "+tid.getId()+" get "+perm+" Lock :"+pid.getPageNumber());
                return true;
            }

            // should not at here
//            System.out.println("acquire Lock exception! ");
            return false;
        } catch (Exception e) {
//            System.out.println("*****"+" tid: "+tid.getId()+" Acquire "+pid.getPageNumber()+"'s "+perm+ " Lock failed "+tryTimes+" times, TX aborted!*****");
            throw new TransactionAbortedException();
        }

    }

    public void waitOrAbort (PageId pid, TransactionId tid, Permissions perm, long startAt) throws TransactionAbortedException, InterruptedException{
        pid.wait(this.waitTimeoutMills + (long)(Math.random() * randomDelayTime));
        if (System.currentTimeMillis() - startAt > this.txFinishTimeoutMills) {
//            System.out.println(" ***** TX aborted as Time Out! *****");
            if (!isFirstLock( pid,  tid,  perm, startAt )) {
                throw new TransactionAbortedException();
            }

        }
    }

    private boolean isFirstLock(PageId pid, TransactionId tid, Permissions perm, long startAt) {
        return  (tx2PageReadLockMap.get(pid)!=null
                && tx2PageReadLockMap.get(pid).size()>=1
                && tx2PageReadLockMap.get(pid).get(0).equals(tid))
                ||
                (tx2PageWriteLockMap.get(pid)!=null
                        && tx2PageWriteLockMap.get(pid).size()>=1
                        && tx2PageWriteLockMap.get(pid).get(0).equals(tid));
    }


    public void addLockIntoPage2TxMap(List<TransactionId> targetList, TransactionId tid, PageId pid, ConcurrentHashMap<PageId, List<TransactionId>> targetMap) {
        if (targetList != null) {
            targetList.add(tid);
        } else {
            targetList = new ArrayList<>();
            targetList.add(tid);
            targetMap.put(pid, targetList);
        }
    }


    public void releaseLock(TransactionId tid, PageId pid) throws Exception {

        // if don't hold lock, throw exception
        if (!hasHoldsLock(tid, pid)) {
//            System.out.println("release Lock失败, tid = " + tid.getId() + "未持有 pid = " + pid.getPageNumber() + "的锁");
            throw new TransactionAbortedException();
        }

        // release READ Lock
        if (page2TxReadLockMap.get(pid)!=null && page2TxReadLockMap.get(pid).contains(tid)) {
            page2TxReadLockMap.get(pid).remove(tid);
        }

        // release WRITE Lock
        if (page2TxWriteLockMap.get(pid)!=null && page2TxWriteLockMap.get(pid).contains(tid)) {
            page2TxWriteLockMap.get(pid).remove(tid);
        }

//        System.out.println("release Lock, tid = " + tid.getId() + "释放 pid = " + pid.getPageNumber() + "的锁,开启Notify");
        synchronized (pid) {
            pid.notifyAll();// TODO !!!!!
        }

    }


    public boolean hasHoldsLock(TransactionId tid, PageId p) {
        return page2TxWriteLockMap.get(p)!=null && page2TxWriteLockMap.get(p).contains(tid)
                || page2TxReadLockMap.get(p)!=null && page2TxReadLockMap.get(p).contains(tid);
    }

}
```







## Lock Lifetime

您将需要实现严格的两阶段锁定。这意味着事务应该在访问任何对象之前获得适当类型的锁，并且在事务提交之前不应该释放任何锁。

幸运的是，SimpleDB的设计使您可以在读取或修改`BufferPool.getPage()`中获取页上的锁。因此，与其在每个操作符中添加对锁定例程的调用，**我们建议在getPage()中获取锁**。根据您的实现，您可能不需要在其他任何地方获得锁。这取决于你来验证这一点!

在读取任何页面(或元组)之前，都需要获得一个共享锁; 在写入任何页面(或元组)之前，都需要获得一个排他锁。你会注意到我们已经在BufferPool中传递Permissions对象了;这些对象表示调用者希望在被访问的对象上拥有的锁类型(我们已经为您提供了Permissions类的代码)。



注意，`HeapFile.insertTuple()`和`HeapFile.deleteTuple()`的实现，以及`HeapFile.iterator()`返回的迭代器的实现应该使用`BufferPool.getPage()`访问页面。

仔细检查getPage()的这些不同用法是否传递了正确的Permission对象(例如，`Permissions.READ_WRITE`或`Permission_READ_ONLY`)。

您可能还希望仔细检查`BufferPool.insertTuple()`和`BufferPool.deleteTupe()`的实现是否在它们访问的任何页面上调用markDirty()(在lab2中实现此代码时应该这样做，但我们没有对此进行测试)。

在获得锁之后，还需要考虑何时释放它们。很明显，您应该在事务提交或终止后释放与事务相关的所有锁，以确保严格的2PL。然而，在其他情况下，在事务结束之前释放锁可能是有用的。例如，您可以在扫描页以找到空槽后释放页上的共享锁(如下所述)。



**Exercise 2**

确保在整个SimpleDB中获取和释放锁。以下的一些(但不一定是全部)操作，你应该验证是否正常工作:

- 在SeqScan期间从页面中读取元组(如果你在BufferPool.getPage()中实现了锁定，只要`HeapFile.iterator()`使用`BufferPool.getPage()`，这应该可以正常工作。)
- 通过BufferPool和HeapFile方法插入和删除元组(如果你在`BufferPool.getpage()`中实现了锁定，只要`HeapFile.inserttuple()`和`HeapFile.deletetuple()`使用`BufferPool.getPage()`，这应该可以正常工作。)

在以下情况下，您还需要特别认真地考虑获取和释放锁:

- 向HeapFile中添加一个新页面。什么时候将页面物理地写入磁盘? 在HeapFile级别是否存在需要特别注意的(在其他线程上的)其他事务的竞争条件，而不考虑页面级别的锁定?
- 寻找可以插入元组的空槽。大多数实现都会扫描页面寻找空槽，这需要一个READ_ONLY锁。然而，令人惊讶的是，如果事务t在页p上没有空闲插槽，t可能会立即释放页p上的锁。虽然这显然与两阶段锁定规则相矛盾，但这是可以的，因为t没有使用来自页面的任何数据，因此`更新页p的并发事务 t' `不可能影响t的答案或结果。

此时，您的代码应该通过LockingTest中的单元测试。



下面这样会有问题，比如两个线程AB都要append page，写一个page到disk上；

```java
// 开始创建一个新的page,准备写到磁盘上;
byte[] empty = HeapPage.createEmptyPageData();
HeapPage heapPage = new HeapPage(new HeapPageId(tableId, numPages()), empty);
heapPage.insertTuple(t);
LockManager.getLock(tid,heapPage.getId(),Permissions.READ_WRITE); // 这里加锁
writePage(heapPage);
res.add(heapPage); // 记得加到res中;
```

线程A的numPages()和线程B的numPages()是一样的，那么他们就会写到同一个位置，一个线程会覆盖另外一个线程，出现了冲突；**这是因为我们加锁是加在page级别的，而不是加在表级别，所以我们无法控制两个新的page之间的冲突；**



所以我决定在这里加一个表级锁，保证不会出现上面的问题;

```java
if(!insertSuccess){
    synchronized (this){
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(f,"rw");
            // 开始创建一个新的page,准备写到磁盘上;
            byte[] empty = HeapPage.createEmptyPageData();
    HeapPage heapPage = new HeapPage(new HeapPageId(tableId, numPages()), empty);
            heapPage.insertTuple(t);
            LockManager.getLock(tid,heapPage.getId(),Permissions.READ_WRITE);
            writePage(heapPage);
            res.add(heapPage); // 记得加到res中;
        }catch (IOException e){
            throw e;
        }
    }
}
```









## Implementing NO STEAL

只有在事务提交后，才会将事务的修改写入磁盘。这意味着我们可以通过丢弃脏页并从磁盘重新读取它们来中止事务。**因此，我们不能把脏页evict掉。**这个策略被称为NO STEAL。

您需要修改BufferPool中的evictPage()方法。特别是，它绝不能驱除脏页。**如果缓冲池中的所有页都是脏的，则应该抛出dbeexception。**<u>如果您的驱逐策略驱逐了一个干净的页面，请注意事务可能已经持有的任何锁，并在您的实现中适当地处理它们。</u>



**Exercise 3**

在BufferPool中的evictPage()方法中实现页面移除所需的逻辑，而不移除脏页。





## Transactions

在SimpleDB中，在每个查询开始时创建TransactionId对象。

该对象被传递给查询中涉及的每个操作符。查询完成后，调用BufferPool方法transactionComplete()。

调用此方法要么提交事务，要么中止事务，由参数标志commit指定。在其执行期间的任何时候，操作符都可能抛出TransactionAbortedException异常，这表明发生了内部错误或死锁。我们为您提供的测试用例创建适当的TransactionId对象，以适当的方式将它们传递给您的操作符，并在查询完成时调用transactionComplete()。

我们还实现了TransactionId。



**Exercise 4**

在BufferPool中实现transactionComplete()方法。注意，transactionComplete有两个版本，一个接受额外的布尔提交参数，另一个不接受。没有附加参数的版本应该始终提交，**因此可以通过调用transactionComplete(tid, true)来实现。**

提交时，应该将与事务关联的脏页刷新到磁盘。中止时，应该通过将页面恢复到其磁盘上状态来恢复事务所做的任何更改。

无论事务是提交还是中止，您都应该释放BufferPool保持的关于事务的任何状态，包括释放事务持有的任何锁。

此时，您的代码应该通过TransactionTest单元测试和AbortEvictionTest系统测试。您可能会发现TransactionTest{One, Two Five, Ten, AllDirty}系统测试具有解释性，但在完成下一个练习之前，它们可能会失败。









## Deadlocks and Aborts

SimpleDB中的事务有可能出现死锁(如果您不明白原因，我们建议您阅读Ramakrishnan & Gehrke中关于死锁的内容)。您将需要检测这种情况并抛出TransactionAbortedException。

有许多可能的方法来检测死锁。一个普通的例子是实现一个简单的超时策略，如果事务在给定的时间内没有完成，该策略将中止事务。对于真正的解决方案，您可以在依赖关系图数据结构中实现周期检测，如课程中所示。在此方案中，您将定期检查依赖关系图中的循环，或者每当您试图授予新锁时，如果存在循环，则终止某些操作。

在检测到死锁存在之后，必须决定如何改善这种情况。假设在事务t等待锁时检测到死锁。如果您想"杀人"，您可能会中止t正在等待的所有事务; 这可能会导致大量的工作没有完成，但你可以保证它会取得进展。或者，您可以决定中止，以给其他事务一个取得进展的机会。这意味着最终用户将不得不重试事务t。

另一种方法是使用全局事务排序，以避免构建等待图。出于性能考虑，这有时是首选的，但是在这种方案下，本来可以成功的事务可能会因错误而中止。例如WAIT-DIE和WOUND-WAIT方案。



**Exercise 5**

在src/simpledb/BufferPool.java中实现死锁检测或预防。

对于死锁处理系统，您有许多设计决策，但没有必要做一些非常复杂的事情。我们希望您在每个事务上都能做得比简单的超时更好。一个好的起点是在每个锁请求之前在等待图中实现周期检测，这样的实现将使您获得满分。请在实验报告中描述你的选择，并列出你的选择与其他选择相比的优点和缺点。

您应该通过抛出TransactionAbortedException异常来确保您的代码在发生死锁时正确地中止事务。此异常将由执行事务的代码捕获(例如，TransactionTestUtil.java)，它应该调用transactionComplete()在事务结束后进行清理。您不需要实现自动重新启动由于死锁而失败的事务—您可以假设高级代码将处理这个问题。

我们在test/simpledb/DeadlockTest.java中提供了一些(非单元)测试。

它们实际上有点复杂，因此运行它们可能需要几秒钟以上的时间(取决于您的策略)。如果它们似乎无限期地挂起，那么您可能有一个未解决的死锁。这些测试构造了您的代码应该能够逃脱的简单死锁情况。

注意，在DeadLockTest.java顶部附近有两个定时参数; 这些参数确定测试检查是否已获得锁的频率，以及中止事务重新启动前的等待时间。如果使用基于超时的检测方法，可以通过调整这些参数观察到不同的性能特征。测试将输出对应于已解决死锁的TransactionAbortedExceptions到控制台。

您的代码现在应该通过TransactionTest{One, Two, Five, Ten, AllDirty}系统测试(根据您的实现，该测试也可能运行相当长的时间)。

此时，您应该拥有一个可恢复的数据库，也就是说，如果数据库系统崩溃(在transactionComplete()以外的点)或如果用户显式中止事务，则任何正在运行的事务的影响在系统重新启动(或事务中止)后都将不可见。您可能希望通过运行一些事务并显式地关闭数据库服务器来验证这一点。