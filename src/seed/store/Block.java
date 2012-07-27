package seed.store;

import java.nio.ByteBuffer;

/**
 * <pre>
 * 此Block抽象成为Meta|Data区域,
 * 其中Meta区域包含将所有block串联起来的指针/个数信息,格式固定
 * 其中Data区域抽象为实际的数据载荷区域,但对于key/value来说,会自行定义此区域的存储方式,
 *      但在抽象层面上它们都是有效数据载荷
 *</pre>
 * @author seedshao
 *
 */
public class Block
{
    public static final Block NILL= new Block();
    public static final Block NOT_ENOUGH = new Block();

    public static final byte[] emptyK = new byte[0];
    public static final byte[] emptyV = new byte[0];
    /**
     * 1.block固定的meta data区域
     * 4byte -- pointer to next block(nextPointer)
     * 4byte -- key 占用的字节数(bytes)
     *
     * 2.block的数据区域
     * 4byte -- pointer to data (vPointer)[注意当keyLen>0时,此4byte才是vPointer]
     * .... -- key的数据区域
     *
     * 不要修改它的limit等值
     */
    public static final int POS_NO = 0;
    public static final int POS_LEN = 4;
    public static final int POS_DATA = 8;

    final int blockNo;	// 当前节点的no
    final ByteBuffer bb;
    private Block next;	// 后续节点
    private int preNo;	// 前置节点的no

    private Block(){
        blockNo = -1;
        bb = null;
    }

    public Block(int bno, ByteBuffer bb)
    {
        blockNo = bno;
        this.bb = bb;
    }

    int getNextBNO()
    {
        return getNextBNO(bb);
    }
    int getLen()
    {
        return getLen(bb);
    }
    void setLen(int v)
    {
        setLen(bb, v);
    }
    /**
     * 设置nextBNO，并且设置next
     * @param b
     */
    void setNext(Block b)
    {
        setNextBNO(bb, b.blockNo);
        join(b);
    }
    /**
     * 设置当前节点的next,和b的preNo
     * @warning : 此方法只在初始化block分析时用到
     * @param b
     */
    void join(Block b)
    {
        next = b;
        if(b != null)
        	b.preNo = blockNo;
    }
    Block getNext()
    {
        return next;
    }
    int getPreNo()
    {
    	return preNo;
    }

    void free()
    {
    	next = null;
    	preNo = 0;
        for(int i=0;i<getMetaSize();i++)
            bb.put(i, (byte)0);
        
    }
    void markAsUsed()
    {
        bb.putInt(POS_NO, -1);
    }
    
    
    //// -------下面涉及实际数据区的操作,由Key/Value自行定义
    
    /**
     * @param position
     * @param dst
     */
    int _readAt(int position, ByteBuffer dst)
    {
        int k = 0;
        bb.position(position);
        while(dst.position()<dst.capacity() && bb.hasRemaining())
        {
            dst.put(bb.get());
            k ++;
        }
        return k;
    }
    /**
     *  从v的offset处开始写入数据到buffer的position处开始,返回写入的长度
     * @param v
     * @param offset
     * @return
     */
    int _writeAt(int position, byte[] v, int offset)
    {

        int keyBytes = bb.capacity() - (position > POS_DATA ? position : POS_DATA );
        int length = v.length - offset;
        length = keyBytes > length ? length : keyBytes;
        bb.position(position);
        bb.put(v, offset, length);
        return length;
    }

    //// ------- 静态方法
    static int getMetaSize()
    {
        return POS_DATA;
    }
    static int getNextBNO(ByteBuffer bb)
    {
        return bb.getInt(POS_NO);
    }
    static void setNextBNO(ByteBuffer bb, int bno)
    {
        bb.putInt(POS_NO, bno);
    }
    static int getLen(ByteBuffer bb)
    {
        return bb.getInt(POS_LEN) ;
    }
    static void setLen(ByteBuffer bb, int v)
    {
        bb.putInt(POS_LEN, v);
    }

    public String toString()
    {
        return "Block[bno="+blockNo+",buffer="+bb+",nextbno="+getNextBNO()+",preNo="+getPreNo()+",len="+getLen()+"]";
    }
    
}
