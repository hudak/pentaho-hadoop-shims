package org.pentaho.hadoop.shims.verification;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.logging.CentralLogStore;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.parameters.UnknownParamException;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;

@RunWith(value = Parameterized.class)
public class KitchenTest {

  private File file;

  public KitchenTest(File file) {
    this.file = file;
  }

  @Parameters(name = "{index}: file:{0}")
  public static Collection<Object[]> data() {
    final File dirName = new File(System.getProperty("job.dirname"));
    List<File> files = new ArrayList<File>(Arrays.asList(dirName.listFiles(new FileFilter() {

      @Override
      public boolean accept(File pathname) {
        return pathname.getParentFile().equals(dirName) && (pathname.getName().endsWith(".kjb"));
      }
    })));
    String priorityList = System.getProperty("job.order");
    if (priorityList != null) {
      final List<String> priorities = new ArrayList<String>(Arrays.asList(priorityList.split(",")));
      Collections.sort(files, new Comparator<File>() {

        @Override
        public int compare(File o1, File o2) {
          int indexO1 = priorities.indexOf(o1.getName());
          int indexO2 = priorities.indexOf(o2.getName());
          if (indexO1 == -1 && indexO2 == -1) {
            return o1.getAbsolutePath().compareTo(o2.getAbsolutePath());
          } else if (indexO1 == -1) {
            return 1;
          } else if (indexO2 == -1) {
            return -1;
          } else {
            return indexO2 - indexO1;
          }
        }
      });
    }

    Object[][] data = new Object[files.size()][];
    for (int i = 0; i < files.size(); i++) {
      data[i] = new Object[] { files.get(i) };
    }
    return Arrays.asList(data);
  }

  @Before
  public void setup() throws KettleException {
    KettleEnvironment.init();
  }

  @Test
  public void runTest() throws KettleXMLException, UnknownParamException {
    CentralLogStore.init();
    LogChannelInterface log = new LogChannel("ShimUnittest");
    JobMeta jobMeta = new JobMeta(file.getAbsolutePath(), null, null);
    Job job = new Job(null, jobMeta);
    jobMeta.setArguments(null);
    job.initializeVariablesFrom(null);
    job.setLogLevel(log.getLogLevel());
    jobMeta.setInternalKettleVariables(job);

    for (String parameterName : jobMeta.listParameters()) {
      String systemValue = System.getProperty(parameterName);
      if (systemValue != null) {
        jobMeta.setParameterValue(parameterName, systemValue);
      }
    }

    job.copyParametersFrom(jobMeta);
    job.activateParameters();
    job.start();
    job.waitUntilFinished();
    Assert.assertEquals("Job executed with " + job.getResult().getNrErrors() + ":\n" + job.getResult().getLogText(), 0,
        job.getResult().getNrErrors());
  }
}
