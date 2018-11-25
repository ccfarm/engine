package com.alibabacloud.polar_race.engine.common;

public class Visitor extends AbstractVisitor {
    @Override
    public void visit(byte[] key, byte[] value) {
        for (int j = 0; j < 8; j++) {
            if (key[j] != value[j]) {
                System.out.println(key[j] + " != " + value[j]);
                Util.printBytes(key);
                System.out.println("not pass key");
                Util.printBytes(value);
                System.out.println("not pass value");
                break;
            }
            //System.out.println("pass");
        }
    }
}
