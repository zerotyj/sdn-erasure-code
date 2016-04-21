package cn.edu.hust.xie.localityencode.mr;

import java.util.ArrayList;
import java.util.List;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author yvanhom
 */
public class RaidUtils {
    public static String[] exactRackFromTPath(String[] ls) {
        List<String> result = new ArrayList<String>(ls.length);
        for(String l : ls) {
            int index = l.indexOf('/', 1);
            String rack = l.substring(0, index);
            if(!result.contains(rack)) {
                result.add(rack);
            }
        }
        return result.toArray(new String[0]);
    }
    
    public static void addEmptyBlock(TaskInfo t, int stripeCount) {
        // 用空块补足数据块
        for (int i = t.getBlocks().size(); i < stripeCount; i++) {
            t.addBlock(BlockInfo.EMPTY_BLOCK_INFO);
        }
    }
}
