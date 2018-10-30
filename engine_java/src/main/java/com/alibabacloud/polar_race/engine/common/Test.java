package com.alibabacloud.polar_race.engine.common;

public class Test {
    public static void main(String[] args) {
        EngineRace client = new EngineRace();
        try {
            client.open("");
            for (int i = 0; i < 100; i++) {
                byte[] key = new byte[8];
                byte[] value = new byte[4 * 1024];
                key[0] = (byte)i;
                value[0] = (byte)i;
                client.write(key, value);
            }
        }
        catch (Exception e){

        }

        EngineRace client2 = new EngineRace();
        try {
            client.open("");
            for (int i = 0; i < 100; i++) {
                byte[] key = new byte[8];
                byte[] value;
                key[0] = (byte)i;
                value = client.read(key);
                //System.out.println(value[0]);

            }
        }
        catch (Exception e){
            //System.out.println(e.getMessage());
        }

    }
}
