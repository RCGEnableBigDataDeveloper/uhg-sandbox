import com.protegrity.hadoop.mapreduce.*;

public class ProtegrityMapReduceProtector implements Protector {

	ptyMapReduceProtector mapReduceProtector = null;

	public ProtegrityMapReduceProtector() {
		mapReduceProtector = new ptyMapReduceProtector();
	}

	@Override
	public String unProtect(String token) {
		byte[] byteinput = token.getBytes();
		byte[] bdetoken = mapReduceProtector.unprotect("DE_PAN23", byteinput);
		String detoken = new String(bdetoken);
		return detoken;
	}

	@Override
	public String protect(String number) {
		byte[] byteinput = number.getBytes();
		byte[] bdetoken = mapReduceProtector.protect("DE_PAN23", byteinput);
		String token = new String(bdetoken);
		return token;
	}

	@Override
	public void close() {
		 mapReduceProtector.closeSession();
	}
}
