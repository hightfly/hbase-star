package cn.com.hbase.moniter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


public class MonitorMetrics {

  List<Map<String, Object>> beans = new ArrayList<>();

  public List<Map<String, Object>> getBeans() {
    return beans;
  }

  public void setBeans(List<Map<String, Object>> beans) {
    this.beans = beans;
  }

  public Object getMetricsValue(String name) {
    if (beans.isEmpty()) {
      return null;
    }
    return beans.get(0).getOrDefault(name, null);
  }
}
