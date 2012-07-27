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

import seed.store.PersistKey.PKItr;
import seed.store.PersistValue.PVItr;
import seed.utils.P;
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
	
    protected  static final int lsize = 97;
    private ReentrantReadWriteLock locker = new ReentrantReadWriteLock();
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
        P<Block, Integer> p = PK.getVNO(h, k);
        if (p != null)
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
        P<Block, Integer> p = PK.getVNO(h, k);
        if(p == null)
            p = P.join(PK.add(h, k), 0);
        if(p.a == null || p.a == Block.NOT_ENOUGH)
            return false;
        // 创建数据
        Block vb = PV.add(p.b, v);
        if(vb == null || vb==Block.NOT_ENOUGH)
            return false;
        // 回写索引
        PK.writeVBNO(p.a, vb.blockNo);
        return true;
    }

    public byte[] get(byte[] k)
    {
        int h = Utils.hash(k);
        System.out.println("start_11"+Utils.join(k, ","));
        P<Block, Integer> p = PK.getVNO(h, k);
        if(p == null)
            return null;
        System.out.println("start_12="+p);
        return PV.read(p.b);
    }

    public byte[] remove(byte[] k)
    {
        int h = Utils.hash(k);
        P<Block, Integer> p = PK.getVNO(h, k);
        if(p == null)
            return null;
        return PV.remove(p.b);
    }

    public Iterator<byte[]> keyIterator()
    {
        return new KeyItrWrapper();
    }
    
    public PKItr testPKItr(){
    	return PK.new PKItr();
    }
    
    public PVItr testPVItr(){
    	return PV.new PVItr();
    }
    
    public void print()
    {
    	log.info("----------store_start-----------");
    	// base信息
    	PK.print();
    	log.info("----------store_end-----------");
    }

    /**
     * key上的迭代器
     * @author seedshao
     *
     */
    class KeyItrWrapper implements Iterator<byte[]>{

        PKItr pkItr = PK.new PKItr();

        public boolean hasNext() {
            locker.readLock().lock();
            try
            {
                return pkItr.hasNext();
            } finally
            {
                locker.readLock().unlock();
            }
        }

        public byte[] next() {
            locker.readLock().lock();
            try
            {
                return pkItr.next();
            } finally
            {
                locker.readLock().unlock();
            }
        }

        public void remove() {
            locker.writeLock().lock();
            try
            {
                pkItr.remove();
            } finally
            {
                locker.writeLock().unlock();
            }
        }
    }
}
