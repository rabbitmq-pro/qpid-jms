package org.apache.qpid.jms.support;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public final class RabbitMqCli {

  private static final String DOCKER_PREFIX = "DOCKER:";

  private RabbitMqCli() { }

  static void startBroker() {
    rabbitmqctl("start_app");
  }

  static void stopBroker() {
    rabbitmqctl("stop_app");
  }

  static ProcessState rabbitmqctl(String command) {
    return executeCommand(rabbitmqctlCommand() + " " + command);
  }

  public static String rabbitmqctlCommand() {
    String rabbitmqCtl = System.getProperty("rabbitmqctl.bin");
    if (rabbitmqCtl == null) {
      rabbitmqCtl = "DOCKER:rabbitmq";
    }
    if (rabbitmqCtl.startsWith(DOCKER_PREFIX)) {
      String containerId = rabbitmqCtl.split(":")[1];
      return "docker exec " + containerId + " rabbitmqctl";
    } else {
      return rabbitmqCtl;
    }
  }

  private static ProcessState executeCommand(String command) {
    return executeCommand(command, false);
  }

  private static ProcessState executeCommand(String command, boolean ignoreError) {
    Process pr = executeCommandProcess(command);
    InputStreamPumpState inputState = new InputStreamPumpState(pr.getInputStream());
    InputStreamPumpState errorState = new InputStreamPumpState(pr.getErrorStream());

    int ev = waitForExitValue(pr, inputState, errorState);
    inputState.pump();
    errorState.pump();
    if (ev != 0 && !ignoreError) {
      throw new RuntimeException(
          "unexpected command exit value: "
              + ev
              + "\ncommand: "
              + command
              + "\n"
              + "\nstdout:\n"
              + inputState.buffer.toString()
              + "\nstderr:\n"
              + errorState.buffer.toString()
              + "\n");
    }
    return new ProcessState(inputState);
  }

  private static Process executeCommandProcess(String command) {
    String[] finalCommand;
    if (System.getProperty("os.name").toLowerCase().contains("windows")) {
      finalCommand = new String[4];
      finalCommand[0] = "C:\\winnt\\system32\\cmd.exe";
      finalCommand[1] = "/y";
      finalCommand[2] = "/c";
      finalCommand[3] = command;
    } else {
      finalCommand = new String[3];
      finalCommand[0] = "/bin/sh";
      finalCommand[1] = "-c";
      finalCommand[2] = command;
    }
    try {
      return Runtime.getRuntime().exec(finalCommand);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static int waitForExitValue(
      Process pr, InputStreamPumpState inputState, InputStreamPumpState errorState) {
    while (true) {
      try {
        inputState.pump();
        errorState.pump();
        pr.waitFor();
        break;
      } catch (InterruptedException ignored) {
      }
    }
    return pr.exitValue();
  }

  static List<QueueInfo> listQueues() {
    String output =
        rabbitmqctl("list_queues -q name,messages,messages_ready,messages_unacknowledged,exclusive").output();
    String[] allLines = output.split("\n");
    List<QueueInfo> result = new ArrayList<>();
    for (int i = 1; i < allLines.length; i++) {
      String line = allLines[i];
      if (line != null && !line.trim().isEmpty()) {
        String[] columns = line.split("\t");
        result.add(
            new QueueInfo(
                columns[0],
                Integer.parseInt(columns[1]),
                Integer.parseInt(columns[2]),
                Integer.parseInt(columns[3]),
                Boolean.parseBoolean(columns[4])));
      }
    }
    return result;
  }

  static QueueInfo queueInfo(String q) {
    return listQueues().stream().filter(info -> q.equals(info.name())).findFirst().get();
  }

  static class ProcessState {

    private final InputStreamPumpState inputState;

    ProcessState(InputStreamPumpState inputState) {
      this.inputState = inputState;
    }

    String output() {
      return inputState.buffer.toString();
    }
  }

  private static class InputStreamPumpState {

    private final BufferedReader reader;
    private final StringBuilder buffer;

    private InputStreamPumpState(InputStream in) {
      this.reader = new BufferedReader(new InputStreamReader(in));
      this.buffer = new StringBuilder();
    }

    void pump() {
      String line;
      while (true) {
        try {
          if ((line = reader.readLine()) == null) break;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        buffer.append(line).append("\n");
      }
    }
  }

  public static class QueueInfo {
    private final String name;
    private final int messsageCount;
    private final int readyMessageCount;
    private final int unackedMessageCount;
    private final boolean temporary;

    QueueInfo(String name, int messsageCount, int readyMessageCount, int unackedMessageCount, boolean exclusive) {
      this.name = name;
      this.messsageCount = messsageCount;
      this.readyMessageCount = readyMessageCount;
      this.unackedMessageCount = unackedMessageCount;
      this.temporary = exclusive;
    }

    public String name() {
      return name;
    }

    int messsageCount() {
      return messsageCount;
    }

    int readyMessageCount() {
      return readyMessageCount;
    }

    int unackedMessageCount() {
      return unackedMessageCount;
    }

    public boolean isTemporary() {
      return temporary;
    }
  }
}
