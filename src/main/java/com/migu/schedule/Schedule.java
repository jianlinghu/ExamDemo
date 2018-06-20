package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 *类名和方法不能修改
 */
public class Schedule {

  //节点id列表
  private List<Integer> nodeIds = new ArrayList<Integer>();
  //任务id列表
  private List<Integer> taskIds = new ArrayList<Integer>();
  //任务表
  private Map<Integer, Integer> taskMap = new HashMap<Integer, Integer>();
  //任务状态表
  private Map<Integer, List<TaskInfo>> taskStatus = new HashMap<Integer, List<TaskInfo>>();
  private int threshold = 0;

  public int init() {
    return ReturnCodeKeys.E001;
  }

  /**
   * 服务节点注册
   */
  public int registerNode(int nodeId) {
    if (nodeId < 0) {
      return ReturnCodeKeys.E004;//节点非法
    } else if (nodeIds.contains(nodeId)) {
      return ReturnCodeKeys.E005;//服务节点已注册
    }
    nodeIds.add(nodeId);
    Collections.sort(nodeIds);
    return ReturnCodeKeys.E003;//注册成功
  }

  /**
   * 删除服务节点
   */
  public int unregisterNode(int nodeId) {
    if (nodeId < 0) {
      return ReturnCodeKeys.E004;//节点非法
    } else if (!nodeIds.contains(nodeId)) {
      return ReturnCodeKeys.E007; //节点不存在
    }
    nodeIds.remove(new Integer(nodeId));//删除节点
    return ReturnCodeKeys.E006;
  }

  /**
   * 添加任务
   */
  public int addTask(int taskId, int consumption) {
    if (taskId <= 0) {
      return ReturnCodeKeys.E009;//任务编号非法
    }
    if (taskIds.contains(taskId)) {
      return ReturnCodeKeys.E010;//任务已经添加
    }
    taskIds.add(taskId);
    taskMap.put(taskId, consumption);
    return ReturnCodeKeys.E008;//任务添加成功
  }

  /**
   * 删除任务
   */
  public int deleteTask(int taskId) {
    if (taskId <= 0) {
      return ReturnCodeKeys.E009;//任务编号非法
    }
    if (!taskIds.contains(taskId)) {
      return ReturnCodeKeys.E012;//任务未添加
    }
    taskIds.remove(new Integer(taskId));
    taskMap.remove(new Integer(taskId));
    return ReturnCodeKeys.E011;//任务删除成功
  }

  /**
   * 任务调度
   */
  public int scheduleTask(int threshold) {
    if (threshold < 0) {
      return ReturnCodeKeys.E002;//调度阈值非法
    }
    ;
    this.threshold = threshold;
    boolean balanced = false;

    List<Integer> tmpTasks = new ArrayList<Integer>();
    for (Integer taskId : taskIds) {
      tmpTasks.add(taskId);
    }
    for (Integer nodeId : nodeIds) {
      List<TaskInfo> taskInfos = new ArrayList<TaskInfo>();
      taskStatus.put(nodeId, taskInfos);
    }

    return ReturnCodeKeys.E013;
  }


  /**
   * 查询任务状态列表
   */
  public int queryTaskStatus(List<TaskInfo> tasks) {
    for (Integer nodeId : taskStatus.keySet()) {
      tasks.addAll(taskStatus.get(nodeId));
    }
    return ReturnCodeKeys.E015;
  }

}
