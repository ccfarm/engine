package com.alibabacloud.polar_race.engine.common;

import java.util.*;

public class SkipListNode {
    public long key;
    public int value;
    public SkipListNode right;
    public SkipListNode down;


    public SkipListNode(){
    }

    public SkipListNode(long key, int value) {
        this.key = key;
        this.value = value;
        right = null;
        down = null;
    }
}