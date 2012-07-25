package seed.store;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

import seed.utils.P;
import seed.utils.Utils;


public class PersistValue implements PersistConst
{
    Logger log = Logger.getLogger("kvstore");
    LinkedList<Block> poolInFree = new LinkedList<Block>();
    Map<Integer, Block> headInUse = new HashMap<Integer, Block>();
    private final ReentrantReadWriteLock locker = new ReentrantReadWriteLock();

    /**
     * blockBytes = metaBytes + valueBytes
     */
    private final int blockBytes;
    private final int valueBytes ;
    
    private static final int POS_DATA_START = Block.getMetaSize();
    
    int writeV(Block block, byte[] v, int offset)
    {
        return block._writeAt(POS_DATA_START, v, offset);
    }
    
    int readV(Block block, ByteBuffer dst)
    {
        return block._readAt(POS_DATA_START, dst);
    }
    
    public PersistValue(int vBytes, ByteBuffer buffer)
    {
        this.blockBytes = vBytes + POS_DATA_START;
        this.valueBytes = vBytes;
        int freeBytes = buffer.capacity() - buffer.position();
        int cnt = freeBytes / blockBytes, offset = 0;
        for(int i=1;i<=cnt;i++)
        {
            offset = (i-1) * this.blockBytes;
            buffer.position( offset );
            buffer.limit(offset+ this.blockBytes);
            poolInFree.add(new Block(i, buffer.slice()));
        }
    }

    /**
     * 计算需要的block数
     * @param len
     * @return
     */
    private int calcBlockNeed(int len)
    {
        if(len <=0)
            return 0;
        return len / valueBytes + (len%valueBytes==0?0:1);
    }

    /**
     * 查找从b开始的block链上的(最后一个block,及block总个数)
     * @param b
     * @return <block个数, 链上最后一个block>
     */
    private P<Integer, Block> findBlockAndCntInChain(Block b)
    {
        P<Integer, Block> p = P.join(0, null);
        if(b==null)
            return p;
        for(;b != null;b = b.getNext()) {
            p.a ++;
            p.b = b;
        }
        return p;
    }
//    private P<Integer, Block> findBlockAndCntInChain(Block b)
//    {
//        if(b==null)
//            return P.join(0, null);
//        P<Integer, Block> p = P.join(1, b);
//        for(;b != null;) {
//            b = b.getNext();
//            p.a ++;
//            p.b = b;
//        }
//        return p;
//    }

    byte[] read(int vblockNo)
    {
     // 计算有几个block连起来的
        Block b = headInUse.get(vblockNo);
        P<Integer, Block> p = findBlockAndCntInChain(b);
        if(p.a == 0)
            return Block.emptyV;
        
        byte[] v = new byte[b.getLen()];
        ByteBuffer dst = ByteBuffer.wrap(v);
        int len = 0;
        for(;b != null; b=b.getNext())
        {
            len += readV(b, dst);
            if(len >= v.length)
                break;
        }
        return v;
    }
    
    private void recycle(Block b, boolean isHead)
    {
        if(b == null)
            return ;
        b.free();
        if(isHead)
            headInUse.remove(b.blockNo);
        poolInFree.offer(b);
    }

    /**
     * 一边读一边free掉
     * @param vblockNo
     * @param fetchValue : 是否将值读出来
     * @return
     */
    byte[] _remove(int vblockNo, boolean fetchValue)
    {
        // 计算有几个block连起来的
        Block b = headInUse.get(vblockNo);
        P<Integer, Block> p = findBlockAndCntInChain(b);
        if(p.a == 0)
            return Block.emptyV;
        
        // 一边读一边free掉
        boolean isHead = true;
        byte[] v = null;
        ByteBuffer dst = null;
        if(fetchValue)
        {
            v = new byte[b.getLen()];
            dst = ByteBuffer.wrap(v);
        }
        int len = 0;
        for(Block old;b != null; )
        {
            if(dst != null)
            {
                len += readV(b, dst);
                if(len >= v.length)
                    break;
            }
            old = b;
            b = b.getNext();
            recycle(old, isHead);
            isHead = false;
        }
        return v;
    }
    
    byte[] remove(int vbno)
    {
        return _remove(vbno, true);
    }
    
    void remove2(int vbno)
    {
        _remove(vbno, false);
    }

    /**
     * 注意v==null 或v=[]时,是返回失败, 因为这两个情况,不需要申请vblock
     * @param vbno : vbno<=0时,表示直接用新的块
     * @param v
     * @return
     */
    Block add(int vbno, byte[] v)
    {
        if(v == null || v.length == 0)
            return null;
        int n = calcBlockNeed(v.length);
        Block firstb = vbno<=0 ? null : headInUse.get(vbno);
        P<Integer, Block> info = findBlockAndCntInChain(firstb);    // 剩余需要的块数
        n -= info.a;
        //
        if(n > 0) 
        {   // 还需要申请n个块
            locker.writeLock().lock();
            try
            {
                if(n > poolInFree.size())
                    return Block.NOT_ENOUGH;
                Block t;
                for(;n>0;n--) 
                {
                    t = poolInFree.poll();
                    if(t == null)
                        return Block.NOT_ENOUGH;
                    if(firstb == null)   {// 完全是新申请的,作为第一个块
                        info.b = firstb = t;
                    } else {
                        info.b.setNext(t);
                        info.b = t;   // 让info.b始终移到当前链尾
                    }
                }
            } finally {
               locker.writeLock().unlock(); 
            }
        }
        // --到这则p.b肯定不会为null
        Utils.assertTrue(info.b != null, "add:vbno="+vbno+",info="+info);
        // 写入新的数据
        int offset = 0;
        for(Block b = firstb;b != null ;b=b.getNext())
        {
            // 记下数据
            offset += writeV(b, v, offset);
            if(offset >= v.length)
                break;
        }
        firstb.setLen(v.length);
        headInUse.put(firstb.blockNo, firstb);
        return firstb;
    }

}
