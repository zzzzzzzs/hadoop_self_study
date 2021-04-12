package reduceJoin2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupComparator extends WritableComparator {

    public OrderGroupComparator(){
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aOrder =(OrderBean)a;
        OrderBean bOrder =(OrderBean)b;
        //分组只需要按照pid进行分组即可.
        return aOrder.getPid().compareTo(bOrder.getPid());
    }
}
