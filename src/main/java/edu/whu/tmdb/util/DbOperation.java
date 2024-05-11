package edu.whu.tmdb.util;

import edu.whu.tmdb.query.operations.utils.MemConnect;
import edu.whu.tmdb.query.operations.utils.SelectResult;
import edu.whu.tmdb.storage.memory.SystemTable.*;
import edu.whu.tmdb.storage.memory.Tuple;
import org.apache.kafka.common.protocol.types.Field;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class DbOperation {
    /**
     * 给定元组查询结果，输出查询表格
     * @param result 查询语句的查询结果
     */
    public static void printResult(SelectResult result) {
        // 输出表头信息
        StringBuilder tableHeader = new StringBuilder("|");
        for (int i = 0; i < result.getAttrname().length; i++) {
            tableHeader.append(String.format("%-20s", result.getClassName()[i] + "." + result.getAttrname()[i])).append("|");
        }
        System.out.println(tableHeader);

        // 输出元组信息
        for (Tuple tuple : result.getTpl().tuplelist) {
            StringBuilder data = new StringBuilder("|");
            for (int i = 0; i < tuple.tuple.length; i++) {
                data.append(String.format("%-20s", tuple.tuple[i].toString())).append("|");
            }
            System.out.println(data);
        }
    }

    /**
     * 删除数据库所有数据文件，即重置数据库
     */
    public static void resetDB() {
        // 仓库路径
        String repositoryPath = ".";

        // 子目录路径
        String sysPath = repositoryPath + File.separator + "data\\sys";
        String logPath = repositoryPath + File.separator + "data\\log";
        String levelPath = repositoryPath + File.separator + "data\\level";

        List<String> filePath = new ArrayList<>();
        filePath.add(sysPath);
        filePath.add(logPath);
        filePath.add(levelPath);

        // 遍历删除文件
        for (String path : filePath) {
            File directory = new File(path);

            // 检查目录是否存在
            if (!directory.exists()) {
                System.out.println("目录不存在：" + path);
                return;
            }

            // 获取目录中的所有文件
            File[] files = directory.listFiles();
            if (files == null) { continue; }
            for (File file : files) {
                // 删除文件
                if (file.delete()) {
                    System.out.println("已删除文件：" + file.getAbsolutePath());
                } else {
                    System.out.println("无法删除文件：" + file.getAbsolutePath());
                }
            }
        }
    }

    public static void showBiPointerTable() {
        // TODO-task2
        StringBuilder tableHeader = new StringBuilder("|");
        tableHeader.append(String.format("%-20s", "Class Id")).append("|");
        tableHeader.append(String.format("%-20s", "Object Id")).append("|");
        tableHeader.append(String.format("%-20s", "Deputy Id")).append("|");
        tableHeader.append(String.format("%-20s", "Deputy Object Id")).append("|");
        System.out.println(tableHeader);

        for(BiPointerTableItem item : MemConnect.getBiPointerTableList()){
            StringBuilder res = new StringBuilder("|");
            res.append(String.format("%-20s", item.classid)).append("|");
            res.append(String.format("%-20s", item.objectid)).append("|");
            res.append(String.format("%-20s", item.deputyid)).append("|");
            res.append(String.format("%-20s", item.deputyobjectid)).append("|");
            System.out.println(res);
        }
    }

    public static void showClassTable() {
        // TODO-task2
        StringBuilder tableHeader = new StringBuilder("|");
        tableHeader.append(String.format("%-20s", "Class Name")).append("|");
        tableHeader.append(String.format("%-20s", "Class Id")).append("|");
        tableHeader.append(String.format("%-20s", "Attribute Name")).append("|");
        tableHeader.append(String.format("%-20s", "Attribute Id")).append("|");
        tableHeader.append(String.format("%-20s", "Attribute Type")).append("|");
        System.out.println(tableHeader);

        for(ClassTableItem item : MemConnect.getClassTableList()){
            StringBuilder res = new StringBuilder("|");
            res.append(String.format("%-20s", item.classname)).append("|");
            res.append(String.format("%-20s", item.classid)).append("|");
            res.append(String.format("%-20s", item.attrname)).append("|");
            res.append(String.format("%-20s", item.attrid)).append("|");
            res.append(String.format("%-20s", item.attrtype)).append("|");
            System.out.println(res);
        }
    }

    public static void showDeputyTable() {
        // TODO-task2
        StringBuilder tableHeader = new StringBuilder("|");
        tableHeader.append(String.format("%-20s", "Origin Class Id")).append("|");
        tableHeader.append(String.format("%-20s", "Deputy Class Id")).append("|");
        System.out.println(tableHeader);

        for(DeputyTableItem item : MemConnect.getDeputyTableList()){
            StringBuilder res = new StringBuilder("|");
            res.append(String.format("%-20s", item.originid)).append("|");
            res.append(String.format("%-20s", item.deputyid)).append("|");
            System.out.println(res);
        }
    }

    public static void showSwitchingTable() {
        // TODO-task2
        StringBuilder tableHeader = new StringBuilder("|");
        tableHeader.append(String.format("%-20s", "Origin Class Id")).append("|");
        tableHeader.append(String.format("%-20s", "Origin Attribute Id")).append("|");
        tableHeader.append(String.format("%-20s", "Origin Attribute Name")).append("|");
        tableHeader.append(String.format("%-20s", "Deputy Class Id")).append("|");
        tableHeader.append(String.format("%-20s", "Deputy Attribute Id")).append("|");
        tableHeader.append(String.format("%-20s", "Deputy Attribute Name")).append("|");
        System.out.println(tableHeader);

        for(SwitchingTableItem item : MemConnect.getSwitchingTableList()){
            StringBuilder res = new StringBuilder("|");
            res.append(String.format("%-20s", item.oriId)).append("|");
            res.append(String.format("%-20s", item.oriAttrid)).append("|");
            res.append(String.format("%-20s", item.oriAttr)).append("|");
            res.append(String.format("%-20s", item.deputyId)).append("|");
            res.append(String.format("%-20s", item.deputyAttrId)).append("|");
            res.append(String.format("%-20s", item.deputyAttr)).append("|");
            System.out.println(res);
        }
    }

    public static void showObjectTable() {
        // TODO-task2
        StringBuilder tableHeader = new StringBuilder("|");
        tableHeader.append(String.format("%-20s", "Class Id")).append("|");
        tableHeader.append(String.format("%-20s", "Tuple Id")).append("|");
        System.out.println(tableHeader);

        for(ObjectTableItem item : MemConnect.getObjectTableList()){
            StringBuilder res = new StringBuilder("|");
            res.append(String.format("%-20s", item.classid)).append("|");
            res.append(String.format("%-20s", item.tupleid)).append("|");
            System.out.println(res);
        }
    }
}
