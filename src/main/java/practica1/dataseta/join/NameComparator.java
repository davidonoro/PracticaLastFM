package practica1.dataseta.join;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NameComparator extends WritableComparator{
	
	public NameComparator() {
		super(WritableComparable.class);
	}
	
	/** 
	 * Compare Name/Year pairs by comparing the first string (name) ONLY.
	 * This will group keys with the same name value together.
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable v1, WritableComparable v2){

		CustomKey pair1 = (CustomKey)v1;
		CustomKey pair2 = (CustomKey)v2;
		
		return pair1.getUser().compareTo(pair2.getUser());

	}	
	
	/** 
	 * Overrides the default compare method, which is optimized for objects which
	 * can be compared byte by byte.  For Name/Year this isn't the case, so we need
	 * to read the incoming bytes to deserialize the StringPairWritable objects being
	 * compared.
	 */
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		DataInput stream1 = new DataInputStream(new ByteArrayInputStream(b1,
				s1, l1));
		DataInput stream2 = new DataInputStream(new ByteArrayInputStream(b2,
				s2, l2));

		CustomKey v1 = new CustomKey();
		CustomKey v2 = new CustomKey();

		try {
			v1.readFields(stream1);
			v2.readFields(stream2);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return compare(v1, v2);
	}

}
