package simpledb.transaction;

import simpledb.common.Permissions;
import simpledb.storage.PageId;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

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
    long waitTimeoutMills = 200;
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
                List<PageId> tx2PageReadLockList = tx2PageReadLockMap.get(tid);
                addLockIntoTx2PageMap(tx2PageReadLockList,tid,pid,tx2PageReadLockMap);

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
                List<PageId> tx2PageWriteLockList= tx2PageReadLockMap.get(tid);
                addLockIntoTx2PageMap(tx2PageWriteLockList,tid,pid,tx2PageWriteLockMap);
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
            System.out.println("isFirstLock: "+ isFirstLock(pid,  tid,  perm, startAt ));
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

    public void addLockIntoTx2PageMap(List<PageId> targetList, TransactionId tid, PageId pid, ConcurrentHashMap<TransactionId, List<PageId>> targetMap) {
        if (targetList != null) {
            targetList.add(pid);
        } else {
            targetList = new ArrayList<>();
            targetList.add(pid);
            targetMap.put(tid, targetList);
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

    public void releaseLock(TransactionId tid) throws Exception{
//        List<PageId> pageIds = tx2PageReadLockMap.get(tid);
//        if(pageIds != null)
//            for (PageId pid : pageIds){
//                releaseLock(tid,pid);
//                System.out.println("事务"+tid.myid +"释放了页面"+pid.getPageNumber()+"的读锁");
//            }
//
//        pageIds = tx2PageWriteLockMap.get(tid);
//        if(pageIds != null)
//            for (PageId pid : pageIds){
//                releaseLock(tid,pid);
//                System.out.println("事务"+tid.myid +"释放了页面"+pid.getPageNumber()+"的写锁");
//            }
        for (PageId pid: page2TxWriteLockMap.keySet()) {
            if(page2TxWriteLockMap.get(pid).contains(tid))
                releaseLock(tid,pid);
        }
        for (PageId pid: page2TxReadLockMap.keySet()) {
            if(page2TxReadLockMap.get(pid).contains(tid))
                releaseLock(tid,pid);
        }
    }



    public boolean hasHoldsLock(TransactionId tid, PageId p) {
        return page2TxWriteLockMap.get(p)!=null && page2TxWriteLockMap.get(p).contains(tid)
                || page2TxReadLockMap.get(p)!=null && page2TxReadLockMap.get(p).contains(tid);
    }

}


