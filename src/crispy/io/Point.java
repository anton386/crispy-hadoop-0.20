package crispy.io;

public class Point {

    Integer read1;
    Integer read2;

    public Point(Integer read1, Integer read2) {
	this.read1 = read1;
	this.read2 = read2;
    }

    public Integer getRead1() {
	return this.read1;
    }

    public Integer getRead2() {
	return this.read2;
    }

    //public int compareTo(Point p) {
    //	int out = 2;
    //	if (this.distance.equals(p.distance)) {
    //	    out = 0;
    //	}
    //	else if (this.distance > p.distance) {
    //	    out = 1;
    //	}
    //	else if (this.distance < p.distance) {
    //	    out = -1;
    //	}
    //	return out;
    //}

}