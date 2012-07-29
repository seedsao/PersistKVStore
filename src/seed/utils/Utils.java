package seed.utils;

import java.util.Random;

public class Utils
{
    public static boolean isEquals(byte[] a, int aOffset, byte[] b)
    {
        if (a == null || b == null)
            return false;
        if (a.length - aOffset != b.length)
            return false;
        for (int i = 0; i < b.length; i++)
        {
            if (a[i + aOffset] != b[i])
                return false;
        }
        return true;
    }
    
    public static boolean isEquals(byte[] a, byte[] b)
    {
        return isEquals(a, 0, b);
    }
    
    /**
     * 从offset位置开始读一个4字节的int
     * 低位在低
     * 
     * @param a
     * @param offset
     * @return
     */
    public static int readInt(byte[] a, int offset)
    {
        if (a == null || a.length - offset < 4)
            throw new IllegalAccessError("offset:" + offset + ",len=" + a.length);
        int mask = 0x0ff;
        return ((mask & a[offset]) << 24) | ((mask & a[offset + 1]) << 16) | ((mask & a[offset + 2]) << 8) | (mask & a[offset + 3]);
    }
    
    /**
     * 编码成数组
     * 
     * @param v
     * @return
     */
    public static byte[] encodeInt(int v)
    {
        return new byte[] { (byte) ((v >>> 24) & 0xFF), (byte) ((v >>> 16) & 0xFF), (byte) ((v >>> 8) & 0xFF), (byte) ((v >>> 0) & 0xFF) };
    }
    
    /**
     * 从offset位置开始读一个2字节的int
     * 
     * @param a
     * @param offset
     * @return
     */
    public static short readShort(byte[] a, int offset)
    {
        short len = 0;
        for (int i = 1; i >= 0; i--)
        {
            len <<= 8;
            len |= (0x0ff & a[offset + i]);
        }
        return len;
    }
    
    public static int hash(byte[] v)
    {
        if (v == null || v.length == 0)
            return 0;
        int h = 17;
        for (byte b : v)
            h = h * 37 + b;
        return h;
        
//         for collision test
//         return collisionHash(v);
    }
    
    static int collisionHash(byte[] v)
    {
    	return 1;
    }
    
    /**
     * 低位在数组高位
     * 
     * @param i
     * @return
     */
    public static byte[] encodeShort(short i)
    {
        byte[] b = new byte[2];
        for (int k = 0; k < b.length; k++)
        {
            b[k] = (byte) (i & 0x0ff);
            i >>= 8;
        }
        return b;
    }
    
    public static String join(byte[] b, String separator)
    {
        if(b == null)
        	return "";
        String s = "[";
        for (byte k : b)
            s += k + separator;
        s += "]";
        return s;
    }
    
    public static void assertTrue(boolean b, String notTrueMsg)
    {
        if (!b)
            throw new IllegalStateException(notTrueMsg);
    }
    
    public static void main(String[] args)
    {
        int i = 0;
        Random R = new Random();
        for (int jj = 0; jj < 100000000; jj++)
        {
            i = R.nextInt();
            byte[] b1 = encodeInt(i);
            if (i != readInt(b1, 0))
                System.out.println(i);
            // System.out.println(join(b1, ","));
            // System.out.println(readInt(b1, 0));
        }
        
    }
}
