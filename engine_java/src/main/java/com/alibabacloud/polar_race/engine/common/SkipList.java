package com.alibabacloud.polar_race.engine.common;

import java.util.*;

public class SkipList{
    private SkipListNode[] heads;
    private static int maxLevel = 18;
    private static final double PROBABILITY = 0.5;
    private static int[] choiceLevel = new int[]{
            131072,
            65536,
            32768,
            16384,
            8192,
            4096,
            2048,
            1024,
            512,
            256,
            128,
            64,
            32,
            16,
            8,
            4,
            2,
            1
            };
    public SkipList() {
        heads = new SkipListNode[maxLevel];
//        for (int i = 0; i < maxLevel; i++) {
//            heads[i] = new SkipListNode(-1l, -1);
//        }
    }

    public void put(long key, int value) {
        int level = 0;
        double rand = Math.random() * 131072;
        while (level < maxLevel - 1 && rand < choiceLevel[level + 1]) {
            level += 1;
        }
        int startLevel = maxLevel - 1;
        while (startLevel > level && heads[startLevel] == null) {
            startLevel -= 1;
        }
        SkipListNode pre = null;
        SkipListNode up = null;
        while (startLevel > 0) {
            if (pre == null) {
                if (heads[startLevel] == null) {
                    heads[startLevel] = new SkipListNode(key, value);
                }
            }
            while (pre.right != null);
        }
    }
}
