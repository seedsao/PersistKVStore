package seed.store;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import javax.management.RuntimeErrorException;

import org.apache.log4j.Logger;

import seed.store.Block.Holder;
import seed.utils.P;
import seed.utils.Utils;

/**
 * key存储区
 * @author seedshao
 *
 */
public class PersistKey implements PersistConst
{
    Logger log = Logger.getLogger("kvstore");
//    private static final ReentrantReadWriteLock locker = new ReentrantReadWriteLock();
    LinkedList<Block> poolInFree = new LinkedList<Block>();
    Map<Integer, Block> poolHash = new HashMap<Integer, Block>();
//    Map<Integer, Block> poolInUse = new HashMap<Integer, Block>();

    /**
     * blockBytes = metaBytes + keyBytes
     */
    private final int blockBytes ;    // byteSize
    private final int keyBytes ;
    private final int maxBlockCnt ;    // 最大block数
    private final ByteBuffer buffer;    // 存储区

    private static final int LEN_VNO = 4;

    private static final int POS_DATA_START = Block.getMetaSize();
    private static final int POS_DATA_VBNO = POS_DATA_START;                                    // bno的偏移
    private static final int POS_DATA_KEY_FIRST = POS_DATA_START + LEN_VNO;    // 第一个key块
    private static final int POS_DATA_KEY_AFTER = POS_DATA_START ;                      // 后续的key块

    void writeVBNO(Block block, int vbno)
    {
        block._writeAt(POS_DATA_VBNO, Utils.encodeInt(vbno), 0);
    }

    /**
     * 第一个key block因为包括了VBNO,所以必须从POS_DATA_KEY开始
     * @param block
     * @param key
     * @param offset
     * @return
     */
    int writeKatFirstBlock(Block block, byte[] key, int offset)
    {
        return block._writeAt(POS_DATA_KEY_FIRST, key, offset);
    }
    /**
     * 接下来的key block,就直接从POS_DATA_VBNO开始
     * @param block
     * @param key
     * @param offset
     * @return
     */
    int writeKatAfterBlock(Block block, byte[] key, int offset)
    {
        return block._writeAt(POS_DATA_KEY_AFTER, key, offset);
    }
    /**
     * 读第一个keyBlock中的key数据
     * @param block
     * @param dst
     */
//    int readKatFirstBlock(Block block, ByteBuffer dst)
//    {
//        return block._readAt(POS_DATA_KEY_FIRST, dst);
//    }
    /**
     * 读接下来的keyBlock中key的数据
     * @param block
     * @param dst
     */
    int readAll(Block block, ByteBuffer dst)
    {
        return block._readAt(POS_DATA_VBNO, dst);
    }

    public PersistKey(int kBytes, ByteBuffer buffer)
    {
        if(kBytes <= LEN_VNO)
            throw new IllegalArgumentException("PersistKey():keyBytes="+kBytes+"<LEN_VNO="+LEN_VNO);
        this.blockBytes = POS_DATA_START + kBytes ;
        this.keyBytes = kBytes;
        int freeBytes = buffer.capacity() - buffer.position();
        this.maxBlockCnt = freeBytes / this.blockBytes;
        this.buffer = buffer;
        // -- 组装
        int offset = 0;
        Block block ;
        Map<Integer, Block> poolInUse = new HashMap<Integer, Block>();
        for(int i=1;i<=maxBlockCnt;i++)
        {
            offset = (i-1) * this.blockBytes;
            buffer.position( offset );
            buffer.limit(offset+ this.blockBytes);
            block = new Block(i, buffer.slice());

            if(block.getNextBNO() != 0)    // 被占用了
                poolInUse.put(block.blockNo, block);
            else
                poolInFree.add(block);
        }
        // -- 分析并链起来
        for(Block _b : poolInUse.values())
        {
            block = poolInUse.get(_b.getNextBNO());
            _b.join(block);
        }
        // -- 分析并放入hash
        for(Block _b : poolInUse.values())
        {
        	// 要求必须是链上的第一个block,其实getPreNo==0是充要条件了，后面一定会>0
        	if(_b.getPreNo() == 0 && _b.getLen() > 0)
        	{
                byte[] key = readCurrentKey(_b);
                poolHash.put(Utils.hash(key), _b);
        	}
        }
        // -- 释放
        poolInUse.clear();  
        poolInUse = null;
        System.gc();    // 哥来触发一下
    }

    /**
     * 计算需要的block数
     * @param len
     * @return
     */
    private short calcBlockCount(int len)
    {
        if(len <=0)
            return 0;
        return (short)(len / keyBytes + (len%keyBytes==0?0:1));
    }

    private void recycle(Block b)
    {
        if(b == null)
            return ;
        b.free();
        poolInFree.offer(b);
    }
    
    /*
     * 仅读取key部分
     */
    private byte[] readCurrentKey(Block head)
    {
    	int klen = head.getLen();
    	if(klen <= 0)
    		return null;
    	// read into buffer
    	ByteBuffer kbb = ByteBuffer.wrap(new byte[klen + LEN_VNO]);
    	readAhead(head, kbb, new Holder());
    	kbb.position(LEN_VNO);
    	// read key
    	byte[] key = new byte[klen];
    	for(int i=0;i<key.length;i++)
    		key[i] = kbb.get();
    	return key;
    }
    /*
     * 将vno,key都读取
     */
    private P<byte[], Integer> readCurrentKV(Block head)
    {
    	int klen = head.getLen();
    	if(klen <= 0)
    		return null;
    	// read into buffer
    	ByteBuffer kbb = ByteBuffer.wrap(new byte[klen + LEN_VNO]);
        readAhead(head, kbb, new Holder());
        kbb.position(0);
        // read vno
        int vno = kbb.getInt(0);
        // read key
        byte[] key = new byte[klen];
        for(int i=0;i<key.length;i++)
    		key[i] = kbb.get();
        
        return P.join(key, vno);
    }
    /**
     * 从head处开始一直读完此key,返回下一个key,null表明此无更多key了
     * @param head	: 必须是(klen>0的)头块
     * @param kbb	: key将读到此kbb中
     * @param tail 	: 此key的尾节点
     * @return	：返回此冲突链上下一个key, null表明无更多key了
     */
    private Block readAhead(Block head, ByteBuffer kbb, Holder tail)
    {
        int klen = head.getLen();
        Utils.assertTrue(klen>0, "readAhead(),head="+head+",klen="+klen+"<=0");
        // 到这来一定保证klen>0即当前b为key的头块
        klen += LEN_VNO;  // 我们要多读4字节(LEN_VNO)出来
        /*
         *  开始读一个key到kbb
         *  1.读到klen长度停止
         *  2.或读到碰到一个新的key头b.getLen()>0
         *  3.或读到next终止
         */
        kbb.clear();
        int i=0;
        Block b = head;
        for(;i<klen && b!=null;)
        {
            if(head !=b && b.getLen() > 0)    // 不能是第一个,第一个肯定>0
                return b;   // 可以直接跳到下一下,因为i<klen && b!=null
//                log.error("hash:"+hash+",block="+firstb+",is_head");
            i += readAll(b, kbb);
            tail.block = b;
            b = b.getNext();
        }
        // 继续读下一个key
        return b;
    }

    /**
     * 取key的blockNo,如果-1则说明没此key
     * @param hash
     * @param key
     * @return
     */
    protected P<Block, Integer> getVNO(int hash, byte[] key)
    {
        Block b = poolHash.get(hash);
        if(b == null)
            return null;
        
        ByteBuffer kbb = ByteBuffer.wrap(new byte[LEN_VNO+key.length]); // 读key的buffer -- 只读与目标key相同的那些key
        /*
         *  klen : 当前key占用字节数
         */
        Block head;
//        System.out.println("start_21");
        for(int klen = 0 ;b != null;) // 下一个block不存在,可以结束了
        {
            klen = b.getLen();
            /*
             * 1.不是key的第一个block,跳过此block
             * 2.如果实际需要字节数与此key的占用数不=,那么肯定不相等了,直接跳过
             */
//            System.out.println("start_22");
            if(klen <= 0 || key.length != klen) {
                b = b.getNext();
                continue;
            }
//            System.out.println("start_23");
            // head是当前key的第一个结点
            head = b;
            b = readAhead(head, kbb, new Holder());
//            System.out.println("start_24");
            /*
           * 1.与目标key比较
           * 2.b已经是当前key的后继block
           */
            if(Utils.isEquals(kbb.array(), LEN_VNO, key))
                return P.join(head, kbb.getInt(0));

//            // 到这来一定保证klen>0即当前b为key的头块
//            klen += LEN_VNO;  // 我们要多读4字节(LEN_VNO)出来
//            /*
//             *  开始读一个key到kbb
//             *  1.读到klen长度停止
//             *  2.或读到碰到一个新的key头b.getLen()>0
//             *  3.或读到next终止
//             */
//            kbb.clear();
//            int i=0;
//            Block firstb = b;
//            for(;i<klen && b!=null;)
//            {
//                if(firstb !=b && b.getLen() > 0)    // 不能是第一个,第一个肯定>0
//                    continue outter;   // 可以直接跳到下一下,因为i<klen && b!=null
////                    log.error("hash:"+hash+",block="+firstb+",is_head");
//                i += readAll(b, kbb);
//                b = b.getNext();
//            }
//            /*
//             * 1.与目标key比较, 刚好有读完klen过才比较,不然肯定不会相等的
//             * 2.b已经是当前key的后继block
//             * 3.firstb是当前key的第一个结点
//             */
//            if(i==klen && Utils.isEquals(kbb.array(), LEN_VNO, key))
//                return P.join(firstb, kbb.getInt(0));
//            // 继续读下一个key
        }
        return null;
    }

    Block add(int hash, byte[] key)
    {
        short blockNeed = calcBlockCount(LEN_VNO+key.length); //目标key需要多长block才能放得下
        if(blockNeed <0)
        {
            log.warn("add(),hash="+hash+",keyLen="+key.length+",key_to_long_than_"+Short.MAX_VALUE);
            return Block.NOT_ENOUGH;
        }
        if(blockNeed > poolInFree.size())
        {
            log.warn("add(),hash="+hash+",keyLen="+key.length+",no_space");
            return Block.NOT_ENOUGH;
        }
        log.info("<<<<<<use block");
        log.info("<<<<<<write "+Utils.join(key, ","));
        Block b= null, p = null, firstp = null;
        int offset = 0;
        // 存入一个key
        for(int i=0;i<blockNeed;i++)
        {
            b = poolInFree.poll();
            if(b == null)
            {
                // TODO 正常情况不会到达这,需要recycle分配出来的block
                log.error("add(),hash="+hash+",keyLen="+key.length+",no_space,need recycle!");
                return Block.NOT_ENOUGH;
            }
            b.markAsUsed();	// 先标记使用中
            if(p == null)
            {
            	log.info("<<"+b+"~offset:"+offset);
                offset += writeKatFirstBlock(b, key, offset);
                firstp = b;
            } else
            {
            	log.info("<<"+b+"~offset:"+offset+"~pre:"+p);
                offset += writeKatAfterBlock(b, key, offset);
                p.setNext(b);
            }
            p = b;
            if(offset >= key.length)
                break;
        }
        /*
         * p : 此key占用block链上最后一个block
         * firstp : 此key占用block链上第一个block
         */
        firstp.setLen(key.length);
        b = poolHash.get(hash);
        //把自己放最前面
        if(b != null)
            p.setNext(b);
        //放入hash索引表
        poolHash.put(hash, firstp);
        log.info("<<<<use end");
        return firstp;
    }

    boolean remove(int hash, byte[] key)
    {
        Block b = poolHash.get(hash);
        if(b == null)
            return false;
        /*
         * klen : 当前key占用字节数
         * p : 每个key的整个block链接的前继结点,用于删除
         * kbb : 读key的buffer,只读与目标key相同的那些key
         */
        ByteBuffer kbb = ByteBuffer.wrap(new byte[LEN_VNO+key.length]);
        Block pre = null, head;
        Holder preHolder = new Holder();	// 用于记录head的前一个节点
        for(int klen = 0 ;b != null;) // 下一个block不存在,可以结束了
        {
//            klen = b.getLen();
//            /*
//             * 1.不是key的第一个block,跳过此block
//             * 2.如果实际需要字节数与此key的占用数不=,那么肯定不相等了,直接跳过
//             */
//            if(klen <= 0 || key.length != klen)
//                continue;
//            // p只在这里赋值,因为只用记住当前key的父结点
//            p = b;
//            klen += LEN_VNO;  // 我们要多读4字节(LEN_VNO)出来
//            /*
//             *  开始读一个key进kbb
//             *  1.读到klen长度停止
//             *  2.或读到碰到一个新的key头b.getLen()>0
//             *  3.或读到next终止
//             */
//            kbb.clear();
//            int i=0;
//            Block firstb = b;
//            for(;i<klen;)
//            {
//                i += readAll(b, kbb);   // 从当前b(头块)开始读
//                p = b;
//                b = b.getNext();
//                if(b == null)
//                {
//                    log.error("hash:"+hash+",block="+firstb+"not_exist");
//                    return false;
//                }
//                if(b.getLen() > 0)
//                {
//                    log.error("hash:"+hash+",block="+firstb+",is_head");
//                    break;
//                }
//            }
        	
            klen = b.getLen();
            /*
             * 1.不是key的第一个block,跳过此block
             * 2.如果实际需要字节数与此key的占用数不=,那么肯定不相等了,直接跳过
             */
//            System.out.println("start_22");
            if(klen <= 0 || key.length != klen) {
            	pre = b;
                b = b.getNext();
                continue;
            }
//            System.out.println("start_23");
            // head是当前key的第一个结点
            head = b;
            pre = preHolder.block;	// 此head的前继节点
            b = readAhead(head, kbb, preHolder);	// 经过此步后，preHolder记录的是b的前缀，head这个key的最后一个节点

            /*
             * 1.与目标key比较
             * 2.b已经是当前key的后继block
             * 3.firstb是当前key的第一个结点
             */
            if(Utils.isEquals(kbb.array(), LEN_VNO, key))
            { // 执行删除
                if(pre == null){  // 当前key为hash链上第一个key
                    if(b != null) 
                    {
                    	// 将后继key设置到hash查找表中
                    	poolHash.put(hash, b);	// hash已经被替换成新的了，后面不能再删除了
                    } else
                    {
                    	poolHash.remove(hash);	// 当前key为hash链上第一个key,时，需要删除hash
                    }
                } else {
                    pre.setNext(b);   //将后继给链上去
                }
                // 当前key的尾节点断开
                pre = preHolder.block;
                if(pre != null)
                	pre.free();
                // 释放当前的key
                log.info("free block>>>>");
                for(b=head;b != null;b=b.getNext())
                {
                	log.info(">>"+b);
                	recycle(b);
                }
                log.info("free end>>>>");
                return true;
            }
            // 继续读下一个key
        }
        return false;
    }

    public void print()
    {
    	log.info("---------------------PK(poolHashStart)------------------");
    	for(Entry<Integer, Block> e: new TreeMap<Integer, Block>(poolHash).entrySet())
    	{
    		try
    		{
    			P<byte[], Integer> p = readCurrentKV(e.getValue());
	    		log.info(e.getKey()+"~"+e.getValue()+"~"+"~vno="+p.b+"~key="+Utils.join(p.a, ","));
    		}
    		catch(Exception e1)
    		{
    			throw new RuntimeException(e1);
    		}
    	}
    	log.info("---------------------PK(poolHashEnd)------------------");
    }
    
    /**
     * 迭代器
     * @author seed2
     *
     */
    class PKItr extends BlockItr
    {
        public PKItr()
        {
            super(buffer, maxBlockCnt, blockBytes);
        }

        @Override
        public boolean hasNext() {
            return searchNext(false) != null;
        }

        @Override
        public byte[] next() {
            Block b = searchNext(true);
            if(b != null && b.getLen() > 0)
                return readCurrentKey(b);
            return null;
        }

        @Override
        public void remove() {
            // TODO Auto-generated method stub
        }

    }

}
