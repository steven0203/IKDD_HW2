package myudfs;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;



public class DISTANCE extends EvalFunc<Double>
{
	private static double rad(double d) {
		return d * Math.PI / 180.0;
	}
	public Double exec(Tuple input) throws IOException {
		try{
			double EarthRadius=6378.137;
			double latitude1=rad((Double)input.get(0));
			double longitude1=rad((Double)input.get(1));
			double latitude2=rad((Double)input.get(2));
			double longitude2=rad((Double)input.get(3));
			double latitude=Math.abs(latitude2-latitude1);
			double longitude=Math.abs(longitude2-longitude1);
			double result=2*EarthRadius*Math.asin(Math.pow(Math.pow(Math.sin(longitude/2.0),2.0)+Math.cos(longitude1)*Math.cos(longitude2)*Math.pow(Math.sin(latitude/2),2.0),0.5))*1000.0;
			return result;

		}catch(Exception e){
			
			throw new IOException("Caught exception processing input row ");
		}
	}
};
