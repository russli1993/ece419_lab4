
public class Logger {
	public static boolean on = true;

	public static void print(String str) {
		if (on) {
			System.out.println("LOG: "+ str);
		}
	}
}
