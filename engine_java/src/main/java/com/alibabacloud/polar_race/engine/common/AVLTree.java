package com.alibabacloud.polar_race.engine.common;

public class AVLTree {
    private AVLTreeNode mRoot;    // 根结点

    // AVL树的节点(内部类)
    class AVLTreeNode {
        long key;                // 关键字(键值)
        int height;         // 高度
        int value;
        AVLTreeNode left;    // 左孩子
        AVLTreeNode right;    // 右孩子

        public AVLTreeNode(long key, int value, AVLTreeNode left, AVLTreeNode right) {
            this.key = key;
            this.value = value;
            this.left = left;
            this.right = right;
            this.height = 0;
        }
    }

    private int height(AVLTreeNode tree) {
        if (tree != null)
            return tree.height;
        else {
            return 0;
        }
    }

    public int height() {
        return height(mRoot);
    }

    private int max(int a, int b) {
        return a>b ? a : b;
    }

    private AVLTreeNode leftLeftRotation(AVLTreeNode k2) {
        AVLTreeNode k1;

        k1 = k2.left;
        k2.left = k1.right;
        k1.right = k2;

        k2.height = max( height(k2.left), height(k2.right)) + 1;
        k1.height = max( height(k1.left), k2.height) + 1;
        return k1;
    }

    private AVLTreeNode rightRightRotation(AVLTreeNode k1) {
        AVLTreeNode k2;

        k2 = k1.right;
        k1.right = k2.left;
        k2.left = k1;

        k1.height = max( height(k1.left), height(k1.right)) + 1;
        k2.height = max( height(k2.right), k1.height) + 1;

        return k2;
    }

    private AVLTreeNode leftRightRotation(AVLTreeNode k3) {
        k3.left = rightRightRotation(k3.left);

        return leftLeftRotation(k3);
    }

    private AVLTreeNode rightLeftRotation(AVLTreeNode k1) {
        k1.right = leftLeftRotation(k1.right);

        return rightRightRotation(k1);
    }

    private AVLTreeNode insert(AVLTreeNode tree, long key, int value) {
        if (tree == null) {
            tree = new AVLTreeNode(key, value,null, null);
        } else {
            long cmp = key - tree.key;

            if (cmp < 0) {    // 应该将key插入到"tree的左子树"的情况
                tree.left = insert(tree.left, key, value);
                // 插入节点后，若AVL树失去平衡，则进行相应的调节。
                if (height(tree.left) - height(tree.right) == 2) {
                    if (key - tree.left.key < 0)
                        tree = leftLeftRotation(tree);
                    else
                        tree = leftRightRotation(tree);
                }
            } else if (cmp > 0) {    // 应该将key插入到"tree的右子树"的情况
                tree.right = insert(tree.right, key, value);
                // 插入节点后，若AVL树失去平衡，则进行相应的调节。
                if (height(tree.right) - height(tree.left) == 2) {
                    if (key - tree.right.key > 0)
                        tree = rightRightRotation(tree);
                    else
                        tree = rightLeftRotation(tree);
                }
            } else {    // cmp==0
                tree.value = value;
                return tree;
            }
        }

        tree.height = max( height(tree.left), height(tree.right)) + 1;
        return tree;
    }

    public void put(long key, int value) {
        mRoot = insert(mRoot, key, value);
    }

     private int iterativeSearch(AVLTreeNode x, long key) {
        while (x != null) {
            long cmp = key - x.key;
            if (cmp < 0)
                x = x.left;
            else if (cmp > 0)
                x = x.right;
            else
                return x.value;
        }
        return -1;
    }

     public int get(long key) {
        return iterativeSearch(mRoot, key);
     }


}