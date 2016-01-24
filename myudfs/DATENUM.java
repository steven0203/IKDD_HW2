package myudfs;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;



public class DATENUM extends EvalFunc<Integer>
{
	public Integer exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;
		try{
			String str =(String)input.get(0);
			String [] DateNum=str.split("/");
			int result=Integer.parseInt(DateNum[1])*31+Integer.parseInt(DateNum[2]);
			return (Integer)result;
		}catch(Exception e){
			
			throw new IOException("Caught exception processing input row ");
		}
	}
};
