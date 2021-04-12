package test.texttest;

import org.apache.hadoop.io.Text;

public class TextTestMain {
    public static void main(String[] args) {
        Text text = new Text();
        text.set("hello");
        text.set("world");
        System.out.println(text);
    }
}
