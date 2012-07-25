package seed.store;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import seed.Utils.Utils;

public class PersistKVStoreTest
{
    static Logger log;
    static PersistKVStore store;
    static int ksize = 30;
    static int vsize = 100;
    static int fsize = 1000000;
    static Random R = new Random();
    static
    {
        PropertyConfigurator.configure("conf/log4j.properties");
        log = Logger.getLogger("tester");
        //
        new File("d:/t.idx").delete();
        new File("d:/t.dat").delete();
        try
        {
            store = new PersistKVStore("d:/", "t", ksize, vsize, fsize);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            System.out.println("static_init");
        }
    }
    
    public static void main(String[] args)
    {
        
        // 18,113,101,57,|v=103,-60,53,|
      //  store.put(new byte[] { 18, 113, 101, 57 }, new byte[] { 103, -60, 53, });
        //
        singleTest();
        collisionTest();
        // XXX remove方法有问题
        multiTest();
    }
    
    private static void testGet(byte[] k, byte[] v)
    {
        byte[] newV = store.get(k);
        if (Utils.isEquals(v, newV) == false)
        {
            System.out.println("k=" + Utils.join(k, ",") + "|v=" + Utils.join(v, ",") + "|newv=" + Utils.join(newV, ","));
            log.error("k=" + Utils.join(k, ",") + "|v=" + Utils.join(v, ",") + "|newv=" + Utils.join(newV, ","));
        }
    }
    
    // -- 单项测试
    static void singleTest(){
        byte[] k = new byte[] { 18, 113, 101, 57 };
        byte[] v = new byte[] { 103, -60, 53, };
        store.put(k, v);
        testGet(k, v);
    }
    
    // -- 碰撞测试, 将hash方法改为碰撞
    static void collisionTest(){

        // 18,113,101,57,|v=103,-60,53,|
        byte[] k = new byte[] { 18, 113, 101, 57 };
        byte[] v = new byte[] { 103, -60, 53, };
        byte[] k2 = new byte[] { -59,-53,59,-63,-82,78,-121,-43,124,13,-9, };
        byte[] v2 = new byte[] { 51,-12,-21,86,-52,-98,75,-95,87, };
        store.put(k, v);
        store.put(k2, v2);
        
        //
        testGet(k, v);
        testGet(k2, v2);
    }
    
    // -- 多数据量测试(碰撞/与正常)
    static void multiTest()
    {
        for (int i = 0; i < fsize -1; i++)
        {
            byte[] k = new byte[R.nextInt(10*ksize - 1) + 1];
            for (int j = 0; j < k.length; j++)
                R.nextBytes(k);
            byte[] v = new byte[R.nextInt(10*vsize - 1) + 1];
            for (int j = 0; j < v.length; j++)
                R.nextBytes(v);
            byte[] newV = null;
            try
            {
                if (store.put(k, v))
                {
                    newV = store.get(k);
                    if (Utils.isEquals(v, newV) == false)
                    {
                        System.out.println("k=" + Utils.join(k, ",") + "|v=" + Utils.join(v, ",") + "|newv="+newV);
                        log.error("k=" + Utils.join(k, ",") + "|v=" + Utils.join(v, ",") + "|newV="+newV);
                        break;
                    }
                } else
                {
                    // no space
                    break;
                }
            }
            catch (Exception e)
            {
                System.out.println("k=" + Utils.join(k, ",") + "|v=" + Utils.join(v, ",") + "|newV="+newV+ "|e=");
                log.error("k=" + Utils.join(k, ",") + "|v=" + Utils.join(v, ",") + "|newV="+newV+ "|e=" + e);
                System.out.println();
                throw new RuntimeException(e);
            }
            System.out.println("i="+i +"|succ|k="+k.length+",v="+v.length+"|"+",k=" + Utils.join(k, ",") + "|v=" + Utils.join(v, ",") + "|succ");
            //log.info("i="+i +"|succ|k="+k.length+",v="+v.length+"|"+",k=" + Utils.join(k, ",") + "|v=" + Utils.join(v, ",") + "|succ");
        }
    }
}
