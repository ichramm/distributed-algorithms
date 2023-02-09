package dada;

import java.math.BigInteger;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class Primes {

	static Stream<BigInteger> primes() {
		return Stream.iterate(BigInteger.TWO, BigInteger::nextProbablePrime);
	}
	
	
	static long pi(long n) {
		return LongStream.rangeClosed(2, n)
				.parallel() // in this case it works
				.mapToObj(BigInteger::valueOf)
				.filter(i -> i.isProbablePrime(50))
				.count();
	}

	public static void main(String[] args) {
		long start = System.currentTimeMillis();

//		primes().map(p -> BigInteger.TWO.pow(p.intValueExact()).subtract(BigInteger.ONE))
//				//.parallel() (Won't work, compute-ahead strategy takes exponential time)
//				.filter(mersenne -> mersenne.isProbablePrime(50))
//				.limit(20)
//				.forEach(System.out::println);
		
		pi(100000000);

		long finish = System.currentTimeMillis();
		
       
		System.out.println("TimeElapsed: " + (finish - start) + " ms");
	}
}
