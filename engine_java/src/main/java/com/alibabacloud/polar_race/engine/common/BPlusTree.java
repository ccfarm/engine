package com.alibabacloud.polar_race.engine.common;

public class BPlusTree {

    private static final int FACTOR = 5;
    private int MAX_CHILDREN_FOR_INTERNAL = FACTOR;
    private int MAX_FOR_LEAF = FACTOR -1;

    private Node root = null;


    public BPlusTree() {
        this.root = new LeafNode();
    }

    public void put(long key, int value) {
        Node node = this.root.insert(key, value);
        if (node != null) {
            this.root = node;
        }
    }

    public int get(Long key) {
        return this.root.get(key);
    }


    abstract class Node {
        protected Node parent;
        protected long[] keys;
        protected int size;
        abstract Node insert(long key, int value);
        abstract int get(long key);

        int locate(long key) {
            int l = 0;
            int r = this.size - 1;
            while (l <= r) {
                int m = (l + r) / 2;
                if (this.keys[m] == key) {
                    return m;
                } else if (key < this.keys[m]) {
                    r = m - 1;
                } else {
                    l = m + 1;
                }
            }
            if (key > this.keys[r]) {
                return l;
            } else {
                return r;
            }
        }
    }


    class InternalNode extends Node{
        private Node[] pointers;

        public InternalNode() {
            this.size = 0;
            this.pointers = new Node[MAX_CHILDREN_FOR_INTERNAL + 1];
            this.keys = new long[MAX_CHILDREN_FOR_INTERNAL];
        }

        public Node insert(long key, int value) {
            int i = 0;
            for (; i < this.size; i++) {
                if (key < keys[i]) break;
            }
            return this.pointers[i].insert(key, value);
        }

        public int get(long key) {
            int i = 0;
            for (; i < this.size; i++) {
                if (key < keys[i]) break;;
            }
            return this.pointers[i].get(key);
        }

        private Node insert(long key, Node leftChild, Node rightChild) {
            if (this.size == 0) {
                this.size++;
                this.pointers[0] = leftChild;
                this.pointers[1] = rightChild;
                this.keys[0] = key;

                leftChild.parent = this;
                rightChild.parent = this;

                return this;
            }

            long[] newKeys = new long[MAX_CHILDREN_FOR_INTERNAL + 1];
            Node[] newPointers = new Node[MAX_CHILDREN_FOR_INTERNAL + 2];

            int i = 0;
            for (; i < this.size; i++) {
                long curKey = this.keys[i];
                if (curKey > key) break;
            }

            System.arraycopy(this.keys, 0, newKeys, 0, i);
            newKeys[i] = key;
            System.arraycopy(this.keys, i, newKeys, i + 1, this.size - i);

            System.arraycopy(this.pointers, 0, newPointers, 0, i + 1);
            newPointers[i + 1] = rightChild;
            System.arraycopy(this.pointers, i + 1, newPointers, i + 2, this.size - i);

            this.size++;
            if (this.size <= MAX_CHILDREN_FOR_INTERNAL) {
                System.arraycopy(newKeys, 0, this.keys, 0, this.size);
                System.arraycopy(newPointers, 0, this.pointers, 0, this.size + 1);
                return null;
            }

            int m = (this.size / 2);

            // split the internal node
            InternalNode newNode = new InternalNode();

            newNode.size = this.size - m - 1;
            System.arraycopy(newKeys, m + 1, newNode.keys, 0, this.size - m - 1);
            System.arraycopy(newPointers, m + 1, newNode.pointers, 0, this.size - m);

            // reset the children's parent to the new node.
            for (int j = 0; j <= newNode.size; j++) {
                newNode.pointers[j].parent = newNode;
            }

            this.size = m;
            this.keys = new long[MAX_CHILDREN_FOR_INTERNAL];
            this.pointers = new Node[MAX_CHILDREN_FOR_INTERNAL];
            System.arraycopy(newKeys, 0, this.keys, 0, m);
            System.arraycopy(newPointers, 0, this.pointers, 0, m + 1);

            if (this.parent == null) {
                this.parent = new InternalNode();
            }
            newNode.parent = this.parent;

            return ((InternalNode) this.parent).insert(newKeys[m], this, newNode);
        }
    }


    class LeafNode extends Node {
        private int[] values;

        public LeafNode() {
            this.size = 0;
            this.keys = new long[MAX_FOR_LEAF];
            this.values = new int[MAX_FOR_LEAF];
            this.parent = null;
        }

        public Node insert(long key, int value) {
            long[] newKeys = new long[MAX_FOR_LEAF + 1];
            int[] newValues = new int[MAX_FOR_LEAF + 1];

            int i = 0;
            for (; i < this.size; i++) {
                long curKey = this.keys[i];

                if (curKey == key) {
                    this.values[i] = value;
                    return null;
                }

                if (curKey > key) break;
            }

            System.arraycopy(this.keys, 0, newKeys, 0, i);
            newKeys[i] = key;
            System.arraycopy(this.keys, i, newKeys, i + 1, this.size - i);

            System.arraycopy(this.values, 0, newValues, 0, i);
            newValues[i] = value;
            System.arraycopy(this.values, i, newValues, i + 1, this.size - i);

            this.size++;

            if (this.size <= MAX_FOR_LEAF) {
                System.arraycopy(newKeys, 0, this.keys, 0, this.size);
                System.arraycopy(newValues, 0, this.values, 0, this.size);
                return null;
            }

            // need split this node
            int m = this.size / 2;

            this.keys = new long[MAX_FOR_LEAF];
            this.values = new int[MAX_FOR_LEAF];
            System.arraycopy(newKeys, 0, this.keys, 0, m);
            System.arraycopy(newValues, 0, this.values, 0, m);

            LeafNode newNode = new LeafNode();
            newNode.size = this.size - m;
            System.arraycopy(newKeys, m, newNode.keys, 0, newNode.size);
            System.arraycopy(newValues, m, newNode.values, 0, newNode.size);

            this.size = m;

            if (this.parent == null) {
                this.parent = new InternalNode();
            }
            newNode.parent = this.parent;

            return ((InternalNode) this.parent).insert(newNode.keys[0], this, newNode);
        }

        public int get(long key) {
            // two branch search
            if (this.size == 0) return -1;

            int start = 0;
            int end = this.size - 1;

            int middle = (start + end) / 2;

            while (start <= end) {
                long middleKey = this.keys[middle];
                if (key == middleKey) {
                    break;
                }

                if (key < middleKey) {
                    end = middle - 1;
                } else {
                    start = middle + 1;
                }

                middle = (start + end) / 2;
            }

            long middleKey = this.keys[middle];

            return middleKey == key ? this.values[middle] : -1;
        }
    }
}

//public class BPlusTree {
//    static int M = 4;
//    AbstractNode root;
//    private abstract class AbstractNode {
//        public int len;
//        public long[] keys;
//    }
//    private class Leaf extends AbstractNode {
//        public int[] values;
//    }
//    private class Node extends AbstractNode {
//        public AbstractNode[] values;
//    }
//    void insert(AbstractNode node, long key, int value) {
//        if (node.getClass() == Leaf.class) {
//            int l = 0;
//            int r = node.len - 1;
//            while (l < r) {
//                int m = (l + r) / 2;
//                if (node.keys[m] == key) {
//                    ((Leaf) node).values[m] = value;
//                    break;
//                } else if (node.keys[m] > key) {
//                    r = m - 1;
//                } else {
//                    l = m + 1;
//                }
//            }
//            if (node.keys[l] == key) {
//                ((Leaf) node).values[l] = value;
//            } } else if (node.keys[l] > key) {
//                node.len += 1;
//                for (int i = node.len)
//            } else {
//                l = m + 1;
//            }
//
//        }
//    }
//    void put(long key, int value) {
//        if (root == null) {
//            root = new Leaf();
//            root.len = 1;
//            root.keys = new long[M + 1];
//            root.keys[0] = key;
//            ((Leaf) root).values = new int[M + 1];
//            ((Leaf) root).values[0] = value;
//        }
//
//    }
//}
