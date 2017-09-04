package com.hpe.pamirs.schedule.hpeschedule.taskmanager;

/**
 * 
 * @Title :监视器锁对象
 * @author gaojy
 *
 *         Create Date : 2017年8月22日 Update Date :
 */
public class LockObject {

  // 如果使用原子类 AtomicInteger可能更好
  private int m_threadCount = 0;
  private Object m_waitOnObject = new Object();

  public LockObject() {}

  public void waitCurrentThread() throws Exception {
    synchronized (m_waitOnObject) {
      this.m_waitOnObject.wait();
    }
  }

  public void addThread() {
    synchronized (this) {
      m_threadCount++;
    }
  }

  public void realseThread() {
    synchronized (this) {
      m_threadCount--;
    }
  }

  /**
   * 降低线程数量，如果是最后一个线程，则不能休眠
   * 
   * @return
   */
  public boolean realseThreadButNotLast() {
    synchronized (this) {
      return (--this.m_threadCount) != 0 ;
    }
  }
  
  public int count(){
    synchronized(this){
      return m_threadCount;
    }
  }
  
	public void notifyOtherThread() throws Exception {
		synchronized (m_waitOnObject) {
			// System.out.println(Thread.currentThread().getName() + ":" +
			// "唤醒所有等待线程");
			this.m_waitOnObject.notifyAll();
		}
	}

}
