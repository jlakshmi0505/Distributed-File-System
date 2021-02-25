package edu.usfca.cs.chat.util;

import java.util.*;

// courtesy: https://stackoverflow.com/questions/4965335/how-to-print-binary-tree-diagram
public class Tree {

    final String name;
    //    List<Tree> children;
    Map<String, Tree> children;
    Boolean isFile;

    public Tree(String name) {
        this.name = name;
        this.children = new HashMap<>();
        this.isFile = false;
    }

    public Tree(String name, HashMap<String, Tree> children) {
        this.name = name;
        this.children = children;
    }

    public void setChildren(HashMap<String, Tree> children) {
        this.children = children;
    }

    public void addChild(Tree child) {
        this.children.put("", child);
    }

    public String toString() {
        StringBuilder buffer = new StringBuilder();
        print(buffer, "", "");
        return buffer.toString();
    }

    private void print(StringBuilder buffer, String prefix, String childrenPrefix) {
        buffer.append(prefix);
        buffer.append(name);
        buffer.append('\n');
        for (Iterator<Tree> it = this.children.values().iterator(); it.hasNext(); ) {
            Tree next = it.next();
            if (it.hasNext()) {
                next.print(buffer, childrenPrefix + "├── ", childrenPrefix + "│   ");
            } else {
                next.print(buffer, childrenPrefix + "└── ", childrenPrefix + "    ");
            }
        }
    }

    public boolean pathExists(String FilePath) {
        return true;
    }

    public void addFilePath(String filePath) {
        if (filePath.startsWith("/")) {
            filePath = filePath.substring(1);
        }
        String[] filePaths = filePath.split("/");
        addFilePathHelper(this, filePaths, 0);
    }

    private void addFilePathHelper(Tree tree, String[] filePaths, int index) {
        if (index < filePaths.length) {
            // check file is in Tree
            Tree child = null;
            String filePath = filePaths[index];
            if (tree.children.containsKey(filePath)) {
                child = tree.children.get(filePath);
            } else {
                child = new Tree(filePath);
                tree.children.put(filePath, child);
            }

            if (index == filePaths.length - 1) {
                child.isFile = true;
            } else {
                addFilePathHelper(child, filePaths, index + 1);
            }

        }
    }


}