package myudfs;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import java.util.Iterator;

public class GETRESULT extends EvalFunc<DataBag>
{
	public DataBag exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
			return null;
		try{
			DataBag result=(DataBag)input.get(0);
			int target=(int)input.get(1);
			int index=0;
			Iterator it=result.iterator();
			int previousCount=-1;
			long Count;
			Tuple currentTuple;
			while(it.hasNext())
			{
				currentTuple=(Tuple)it.next();
				Count=(long)currentTuple.get(1);
				if((int)Count!=previousCount||index!=target)
				{	
					index++;
				}
				currentTuple.append(index);
				previousCount=(int)Count;
			}
			return result;
		}catch(Exception e){
			
			throw new IOException("Caught exception processing input row ");
		}
	}
};
