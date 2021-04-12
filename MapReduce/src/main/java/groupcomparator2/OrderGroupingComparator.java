package groupcomparator2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator {

    public OrderGroupingComparator(){
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = (OrderBean)a ;
        OrderBean bBean = (OrderBean)b ;

        return aBean.getOrderId().compareTo(bBean.getOrderId()) ;
    }
}
