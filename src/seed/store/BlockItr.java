package seed.store;

import java.nio.ByteBuffer;
import java.util.Iterator;

public abstract class BlockItr  implements Iterator<Block> {
	private ByteBuffer _buffer;
	private int currBlockCnt;
	private int maxBlockCnt;
	private int blockBytes;
	
	protected BlockItr(ByteBuffer buffer, int maxBlockCnt, int blockBytes)
	{
		_buffer = buffer.duplicate();
		this.maxBlockCnt = maxBlockCnt;
		this.blockBytes = blockBytes;
	}
	protected Block searchNext()
	{
		int offset = 0;
        ByteBuffer tmp;
        for(int i=currBlockCnt;i<=maxBlockCnt;i++)
        {
            offset = (i-1) * blockBytes;
            _buffer.position( offset );
            _buffer.limit(offset+ blockBytes);
            tmp = _buffer.slice();
            if(Block.getLen(tmp) > 0)
            {	// 为key的第一块
            	return new Block(i, tmp);
            }
        }
        return null;
	}
}
