package seed.Utils;

/**
 * make use simple
 * @author seedshao
 *
 * @param <A>
 * @param <B>
 */
public class P<A, B>
{
    public A a;
    public B b;

    public P(A a, B b)
    {
        this.a = a;
        this.b = b;
    }

    /**
     * 使调用串变短
     * @param a
     * @param b
     * @return
     */
    public static <A,B> P<A, B> join(A a, B b)
    {
        return new P<A, B>(a, b);
    }
}
