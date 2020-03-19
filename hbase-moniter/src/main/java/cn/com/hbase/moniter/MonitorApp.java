package cn.com.hbase.moniter;

import java.io.IOException;


public class MonitorApp {

  public static void main(String[] args) throws IOException {
    StatefulHttpClient client = new StatefulHttpClient(null);
    HadoopUtil.getHdfsSummary(client).printInfo();
    HBaseUtil.getHbaseSummary(client).printInfo();
  }
}
