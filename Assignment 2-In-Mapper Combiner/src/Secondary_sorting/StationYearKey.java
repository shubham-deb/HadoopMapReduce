package pckg4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class StationYearKey implements WritableComparable<StationYearKey>{

	private String stationId;
	private String year;
	
	public StationYearKey() {}
	
	public StationYearKey(String stationId,String year) {
		this.stationId = stationId;
		this.year = year;
	}
	
	public void readFields(DataInput in) throws IOException {
		stationId = WritableUtils.readString(in);
		year = WritableUtils.readString(in);
	}

	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, stationId);
		WritableUtils.writeString(out,year);
	}

	public int compareTo(StationYearKey o) {
		int result = stationId.compareTo(o.stationId);
		if(result == 0) {
			result = year.compareTo(o.year);
		}
		return result;
	}
	
	   @Override
	    public boolean equals(Object o) {
	        if (this == o) return true;
	        if (o == null || getClass() != o.getClass()) return false;

	        StationYearKey that = (StationYearKey) o;

	        if (stationId != null ? !stationId.equals(that.stationId) : that.stationId != null) return false;
	        if (year != null ? !year.equals(that.year) : that.year != null) return false;

	        return true;
	}
	
	public String toString() {
		return (new StringBuilder()).append(this.stationId)
									.append(",")
									.append(this.year)
									.toString();
	}
	
	public String getStationId() {
		return this.stationId;
	}
	
	public void setStationId(String stationId) {
		this.stationId = stationId;
	}
	
	public String getYear() {
		return this.year;
	}
	
	public void setYear(String year) {
		this.year = year;
	}
}
