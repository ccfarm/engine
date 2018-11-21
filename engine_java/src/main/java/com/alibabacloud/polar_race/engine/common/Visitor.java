package com.alibabacloud.polar_race.engine.common;

public class Visitor extends AbstractVisitor {
    @Override
    public void visit(byte[] key, byte[] value) {
        for (int j = 0; j < 8; j++) {
            if (key[j] != value[j]) {
                for (int k = 0; k < 8; k++) {
                    System.out.print(key[k]);
                }
                System.out.println("not pass");
                for (int k = 0; k < 8; k++) {
                    System.out.print(value[k]);
                }
                System.out.println("not pass");
                break;
            }
            //System.out.println("pass");
        }
    }
}
