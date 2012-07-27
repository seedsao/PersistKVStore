package seed.store;

import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.log4j.Logger;

public abstract class BlockItr  implements Iterator<byte[]> {
    private ByteBuffer _buffer;
    private int currBlockCnt ;
    private int maxBlockCnt;
    private int blockBytes;

    protected BlockItr(ByteBuffer buffer, int maxBlockCnt, int blockBytes)
    {
        _buffer = buffer.duplicate();
        currBlockCnt = 1;   // 从1开始
        this.maxBlockCnt = maxBlockCnt;
        this.blockBytes = blockBytes;
    }
    protected Block searchNext(boolean ahead)
    {
        int offset = 0;
        ByteBuffer tmp;
        for(int i=currBlockCnt;i<=maxBlockCnt;i++)
        {
            offset = (i-1) * blockBytes;
            _buffer.position( offset );
            _buffer.limit(offset+ blockBytes);
            tmp = _buffer.slice();
            if(ahead)
            {
                currBlockCnt = i+1;
            }
            if(Block.getLen(tmp) > 0)
            {    // 为key的第一块
                return new Block(i, tmp);
            }
        }
        return null;
    }
    
    public void print(Logger logger)
    {
    	int offset = 0;
        ByteBuffer tmp;
        for(int i=1;i<=maxBlockCnt;i++)
        {
            offset = (i-1) * blockBytes;
            _buffer.position( offset );
            _buffer.limit(offset+ blockBytes);
            tmp = _buffer.slice();
            Block b = new Block(i, tmp);
            logger.info(b);
        }
    }
}
