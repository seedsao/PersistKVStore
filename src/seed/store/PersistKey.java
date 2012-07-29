package seed.store;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

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
    /*
     * 空闲池
     */
    private final LinkedList<Block> poolFree = new LinkedList<Block>();
    /*
     * hash链map,一条链上会有多个key.
     * 新增加key时，采用头插法，放在最前面
     * 条目为(hash, block)
     */
    private final Map<Integer, Block> poolHash = new HashMap<Integer, Block>();
    /*
     * 专门用于key的遍历
     * 条目为(bno, block)
     */
    private final ConcurrentHashMap<Integer, Block> poolKey = new ConcurrentHashMap<Integer, Block>(); 

    /**
     * blockBytes = metaBytes + keyBytes
     */
    private final int blockBytes ;    // byteSize
    private final int keyBytes ;		// 有效key的大小
    private final int maxBlockCnt ;    // 最大block数
    private final ByteBuffer buffer;    // 存储区

    private static final int LEN_VNO = 4;	// VNO（数据block指针大小，int)

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
                poolFree.add(block);
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
        poolFree.offer(b);
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
    	byte[] src = new byte[klen + LEN_VNO];
    	ByteBuffer kbb = ByteBuffer.wrap(src);
    	readAhead(head, kbb, null);
    	// read key
    	byte[] key = new byte[klen];
    	System.arraycopy(src, LEN_VNO, key, 0, klen);
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
    	byte[] src = new byte[klen + LEN_VNO];
    	ByteBuffer kbb = ByteBuffer.wrap(src);
        readAhead(head, kbb, null);
        kbb.position(0);
        // read vno
        int vno = kbb.getInt(0);
        // read key
    	byte[] key = new byte[klen];
    	System.arraycopy(src, LEN_VNO, key, 0, klen);
        
        return P.join(key, vno);
    }
    /**
     * 从head处开始一直读完此key,返回下一个key,null表明此无更多key了
     * @param head	: 必须是(klen>0的)头块
     * @param kbb	: key将读到此kbb中
     * @param tailHd 	: 此key的尾节点,注意当key只占用一个block时，tailHd也指向head
     * @return	：返回此冲突链上下一个key, null表明无更多key了
     */
    private void readAhead(Block head, ByteBuffer kbb, Holder tailHd)
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
                return ;   // 可以直接跳到下一下,因为i<klen && b!=null
            i += readAll(b, kbb);
            if(tailHd != null)
            	tailHd.block = b;	// 不为null，则记录
            b = b.getNext();
        }
        if(b != null && b.getLen()==0)
        {
        	log.error("readAhead(),err occur! maybe not recycle rightly,head="+head+",tail="+tailHd+",nextHead="+b);
        }
        // 继续读下一个key
    }

    /**
     * 取key的blockNo,如果-1则说明没此key
     * @param hash
     * @param key
     * @param hdHolder
     * @return
     */
    int getVNO(int hash, byte[] key, Holder hdHolder)
    {
        Block head = poolHash.get(hash);
        if(head == null)
            return -1;
        
        ByteBuffer kbb = ByteBuffer.wrap(new byte[LEN_VNO+key.length]); // 读key的buffer -- 只读与目标key相同的那些key
        for(int klen = 0 ;head != null;) // 下一个block不存在,可以结束了
        {
        	// klen : 当前key占用字节数
            klen = head.getLen();
            /*
             * 1.不是key的第一个block,跳过此block
             * 2.如果实际需要字节数与此key的占用数不=,那么肯定不相等了,直接跳过
             */
            if(klen <= 0 || key.length != klen) {
                head = head.getNext();
                continue;
            }
            // head是当前key的第一个结点
           readAhead(head, kbb, hdHolder);
           // 与目标key比较
            if(Utils.isEquals(kbb.array(), LEN_VNO, key))
            {
            	hdHolder.block = head;
            	return kbb.getInt(0);
            }
            // 可以继续查下一个head了
            head = hdHolder.block==null ? null : hdHolder.block.getNext();
        }
        return -1;
    }

    Block add(int hash, byte[] key)
    {
        short blockNeed = calcBlockCount(LEN_VNO+key.length); //目标key需要多长block才能放得下
        if(blockNeed <0)
        {
            log.warn("add(),hash="+hash+",keyLen="+key.length+",key_to_long_than_"+Short.MAX_VALUE);
            return Block.NOT_ENOUGH;
        }
        if(blockNeed > poolFree.size())
        {
            log.warn("add(),hash="+hash+",keyLen="+key.length+",no_space");
            return Block.NOT_ENOUGH;
        }
        log.info("use start>>>>>");
        log.info(">>>write "+Utils.join(key, "|"));
        Block b= null, tail = null, hd = null;
        int offset = 0;
        // 存入一个key
        for(int i=0;i<blockNeed;i++)
        {
            b = poolFree.poll();
            if(b == null)
            {
                // TODO 正常情况不会到达这,需要recycle分配出来的block
                log.error("add(),hash="+hash+",keyLen="+key.length+",no_space,need recycle!");
                return Block.NOT_ENOUGH;
            }
            b.markAsUsed();	// 先标记使用中
            if(tail == null)
            {
                offset += writeKatFirstBlock(b, key, offset);
                log.info(">>>"+b+",offset:"+offset);
                hd = b;
            } else
            {
                offset += writeKatAfterBlock(b, key, offset);
                log.info(">>>"+b+",offset:"+offset+",pre:"+tail);
                tail.setNext(b);
            }
            tail = b;
            if(offset >= key.length)
                break;
        }
        /*
         * tail : 此key占用block链上最后一个block
         * hd : 此key占用block链上第一个block
         */
        hd.setLen(key.length);
        b = poolHash.get(hash);
        //把自己放最前面
        if(b != null)
        {
        	tail.setNext(b);
        	 log.info("add(),insert current key to head,hd="+hd+",tail="+tail+",old="+b);
        }
        //放入hash索引表
        poolHash.put(hash, hd);
        poolKey.put(hd.blockNo, hd);
        log.info(">>>>>use end");
        return hd;
    }

    boolean remove(int hash, byte[] key)
    {
        Block hd = poolHash.get(hash);
        if(hd == null)
            return false;
        /*
         * klen : 当前key占用字节数
         * p : 每个key的整个block链接的前继结点,用于删除
         * kbb : 读key的buffer,只读与目标key相同的那些key
         * preHd : 指向hd的前继block
         * nextHd : 指向后继key的头block(注意与后继block的区别,后继key是以key为单位的，每个key有多个链起来的block组成)
         */
        ByteBuffer kbb = ByteBuffer.wrap(new byte[LEN_VNO+key.length]);
        Block preHd = null, nextHd = null;
        Holder tailHd = new Holder();	// 用于记录head的前一个节点
        for(int klen = 0 ;hd != null;) // 下一个block不存在,可以结束了
        {
            klen = hd.getLen();
            /*
             * 1.不是key的第一个block,跳过此block
             * 2.如果实际需要字节数与此key的占用数不=,那么肯定不相等了,直接跳过
             */
            if(klen <= 0 || key.length != klen) {
            	preHd = hd;			// 始终记录前继block，用于删除
                hd = hd.getNext();	// 此时，只能一个block一个block来搜索到下一个头节点
                continue;
            }
            // 到之为止，preHd是hd的前继block
            readAhead(hd, kbb, tailHd);	// 经过此步后，tailHd记录的是head这个key的最后一个节点
            nextHd = tailHd.block == null ? null : tailHd.block.getNext();
            log.info("remove(),search,head="+hd+",tail="+tailHd.block+",nextHead="+nextHd);
            /*
             * 1.与目标key比较
             * 2.nextHead已经是当前key的后继key的头节点(注意与后继block的区别,后继key是以key为单位的，每个key有多个链起来的block组成)
             * 3.head是当前key的第一个结点
             */
            if(Utils.isEquals(kbb.array(), LEN_VNO, key))
            { // 执行删除
            	// 1.如果此key为hash链上第一个，则更新hash表，否则从链上移除
                if(preHd == null){  // 当前head的前继节点preHead=null，说明当前key为hash链上第一个key
                    if(nextHd != null) 
                    {
                    	// 将后继key设置到hash查找表中
                    	if(nextHd.getLen() == 0)
                		{
                    		// xxx error occur -- 这个情况下，放进去对于使用是安全的，但可能有block没正确回收
                		}
                    	poolHash.put(hash, nextHd);	// hash已经被替换成新的了，后面不能再删除了
                    } else
                    {
                    	poolHash.remove(hash);	// 当前key为hash链上第一个key,时，需要删除hash
                    }
                } else {
                    preHd.setNext(nextHd);   //将后继给链上去
                    log.info("remove(),link nextHead to preHead,preHead="+preHd+",nextHead="+nextHd);
                }
                // 2.从poolKey中删除
                poolKey.remove(hd.blockNo);
                // 当前key的尾节点从block链上断开,不然循环起来释放把有效数据给干掉了
                if(tailHd.block != null)
                	tailHd.block.free();
                // 3.释放当前的key,从当前head节点block开始释放此key的block链
                log.info("remove(),recycle start<<<<<");
                for(;hd != null;)
                {
                	log.info("<<<"+hd);
                	preHd = hd;	
                	hd = hd.getNext();
                	recycle(preHd);	// 必须先记住释放的点，然后指针移一位，不能直接释放b,不然释放当前点，把路径切断了...
                }
                log.info("recycle end<<<<<");
                return true;
            }
            // 继续读下一个key
            preHd = tailHd.block;	// preHd还得是tailHd
            hd = nextHd;			// hd设置为nextHd
        }
        return false;
    }
    
    /**
     * 打印当前poolHash中的key
     */
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
     * 迭代器,对mmap的block进行全遍历，不建议使用了
     * @author seed2
     *
     */
    @Deprecated class PKItr extends BlockItr
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
    
    /**
     * fast版本，因为依靠专门的poolKey来完成遍历，减少全遍历中无谓的查找
     * @author seed2
     *
     */
    class FastPKItr implements Iterator<byte[]>
    {
    	private final Iterator<Block> poolKeyItr ;	// 依靠poolKey上的iterator来完成迭代
    	public FastPKItr()
    	{
    		poolKeyItr = poolKey.values().iterator();
    	}

		@Override
		public boolean hasNext() {
			return poolKeyItr.hasNext();
		}

		@Override
		public byte[] next() {
			Block hd = poolKeyItr.next();
			if(hd == null)
				return null;
			int klen = hd.getLen();
			if(klen <= 0)
				poolKeyItr.remove();	// 移除掉吧，反正留着也没用
			return readCurrentKey(hd);			
		}

		@Override
		public void remove() {
			poolKeyItr.remove();
		}
    	
    }

}
