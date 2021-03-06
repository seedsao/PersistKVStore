package seed.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import seed.store.Block.Holder;
import seed.utils.Utils;
/**
 * a persist key-value store basis of mmap
 * TODO 用文件锁加起安全
 * TODO 读写锁也要用起来
 * @author seedshao
 *
 */
public class PersistKVStore
{
	Logger log = Logger.getLogger("kvstore");
	
    protected ReentrantReadWriteLock locker = new ReentrantReadWriteLock();
//    protected  ReentrantReadWriteLock[] locker = new ReentrantReadWriteLock[lsize];
//    ReentrantReadWriteLock getLocker(int hash)
//    {
//        if(hash == Integer.MIN_VALUE)
//            return locker[17];
//        hash = hash<0 ? -hash : hash;
//        return locker[hash % lsize];
//    }

    protected  final PersistKey PK ;
    protected  final PersistValue PV;

    protected  final FileChannel pkChannel;
    protected  final MappedByteBuffer pkBuffer;

    protected final FileChannel pvChannel;
    protected final MappedByteBuffer pvBuffer;

    public PersistKVStore(String path, String fileName, int keyBytes, int valueBytes, int count) throws IOException
    {
        RandomAccessFile raf;
        FileChannel fch ;
        MappedByteBuffer mbb ;

        raf = new RandomAccessFile(new File(path+"/"+fileName+".idx"), "rw");
        fch = raf.getChannel();
        mbb = fch.map(MapMode.READ_WRITE, 0, count * keyBytes);

        PK = new PersistKey(keyBytes, mbb);
        pkChannel = fch;
        pkBuffer = mbb;

        raf = new RandomAccessFile(new File(path+"/"+fileName+".dat"), "rw");
        fch = raf.getChannel();
        mbb = fch.map(MapMode.READ_WRITE, 0, count * keyBytes);

        PV = new PersistValue(valueBytes, mbb);
        pvChannel = fch;
        pvBuffer = mbb;

    }

    public boolean putIfAbsent(byte[] k, byte[] v)
    {
        int h = Utils.hash(k);
        Holder hdHolder = new Holder();
        int vno = PK.getVNO(h, k, hdHolder);
        if (vno > 0)	// 存在此key则不能put
            return false;
        // 创建一个key
        Block kb = PK.add(h, k);
        if(kb == null || kb == Block.NOT_ENOUGH)
            return false;
        // 创建数据
        Block vb = PV.add(0, v);
        if(vb == null || vb==Block.NOT_ENOUGH)
            return false;
        // 回写索引
        PK.writeVBNO(kb, vb.blockNo);
        return true;
    }

    public boolean put(byte[] k, byte[] v)
    {
        int h = Utils.hash(k);
        Holder hdHolder = new Holder();
        int vno = PK.getVNO(h, k, hdHolder);
        if(vno <= 0)
        {
        	hdHolder.block = PK.add(h, k);
        }
        else
        {
//        	System.out.println("conflict");
        }
        if(hdHolder.block == null || hdHolder.block == Block.NOT_ENOUGH)
            return false;
        // 创建数据
        Block vb = PV.add(vno, v);
        if(vb == null || vb==Block.NOT_ENOUGH)
            return false;
        // 回写索引
        log.info("put(),write vno backto keyblock,k="+Utils.join(k, "|")+",v="+Utils.join(v, "|")+"" +
        		",vno="+vb.blockNo+",keyblock="+hdHolder.block+",");
        PK.writeVBNO(hdHolder.block, vb.blockNo);
        return true;
    }

    public byte[] get(byte[] k)
    {
        int h = Utils.hash(k);
        Holder hdHolder = new Holder();
        int vno = PK.getVNO(h, k, hdHolder);
        if(vno <= 0)
        	return null;
//        System.out.println("---->find key,k="+Utils.join(k, ",")+",vno="+vno+",keyHd="+hdHolder.block);
        return PV.read(vno);
    }

    public byte[] remove(byte[] k)
    {
        int h = Utils.hash(k);
        Holder hdHolder = new Holder();
        int vno = PK.getVNO(h, k, hdHolder);
        if(vno <= 0)
        	return null;
        if(!PK.remove(h, k))
        	return null;	
        byte[] v = PV.remove(vno);
        if(v == null){
        	log.error("remove(),k="+Utils.join(k, ",")+",keyHd="+hdHolder.block+",vno="+vno+",key is remove,but value not found");
        	return null;
        }
        return v;
    }

    public Iterator<byte[]> keyIterator()
    {
        return PK.new FastPKItr();
    }
    
    /*********************下面接口用于测试***********************/
    
    public void print()
    {
    	log.info("----------store_start-----------");
    	// base信息
    	PK.print();
    	PV.print();
    	log.info("----------store_end-----------");
    }

//    /**
//     * key上的迭代器
//     * @author seedshao
//     *
//     */
//    class KeyItrWrapper implements Iterator<byte[]>{
//
//        FastPKItr pkItr = PK.new FastPKItr();
//
//        public boolean hasNext() {
//        	
//        }
//
//        public byte[] next() {
//            locker.readLock().lock();
//            try
//            {
//                return pkItr.next();
//            } finally
//            {
//                locker.readLock().unlock();
//            }
//        }
//
//        public void remove() {
//            locker.writeLock().lock();
//            try
//            {
//                pkItr.remove();
//            } finally
//            {
//                locker.writeLock().unlock();
//            }
//        }
//    }
}
