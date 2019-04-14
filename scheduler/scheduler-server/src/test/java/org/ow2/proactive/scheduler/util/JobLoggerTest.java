/*
 * ProActive Parallel Suite(TM):
 * The Open Source library for parallel and distributed
 * Workflows & Scheduling, Orchestration, Cloud Automation
 * and Big Data Analysis on Enterprise Grids & Clouds.
 *
 * Copyright (c) 2007 - 2017 ActiveEon
 * Contact: contact@activeeon.com
 *
 * This library is free software: you can redistribute it and/or
 * modify it under the terms of the GNU Affero General Public License
 * as published by the Free Software Foundation: version 3 of
 * the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 *
 * If needed, contact us to obtain a release under GPL Version 2 or 3
 * or a different license than the AGPL.
 */
package org.ow2.proactive.scheduler.util;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.ow2.proactive.resourcemanager.core.properties.PAResourceManagerProperties;
import org.ow2.proactive.scheduler.common.job.JobId;
import org.ow2.proactive.scheduler.job.JobIdImpl;
import org.ow2.proactive.utils.appenders.FileAppender;


public class JobLoggerTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    File logFolder;

    @BeforeClass
    public static void init() {
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.DEBUG);
    }

    @AfterClass
    public static void wrapup() {
        PAResourceManagerProperties.LOG4J_ASYNC_APPENDER_ENABLED.updateProperty("true");
        PAResourceManagerProperties.LOG4J_ASYNC_APPENDER_CACHE_ENABLED.updateProperty("false");
    }

    @After
    public void clean() {
        Logger.getLogger(JobLogger.class).removeAllAppenders();
    }

    @Test
    public void testGetJobLogFilename() {
        JobId id = new JobIdImpl(1123, "readableName");
        assertThat(JobLogger.getJobLogRelativePath(id), is("1123/1123"));
    }

    @Test
    public void testLoggerAsync() throws IOException {

        PAResourceManagerProperties.LOG4J_ASYNC_APPENDER_ENABLED.updateProperty("true");
        PAResourceManagerProperties.LOG4J_ASYNC_APPENDER_CACHE_ENABLED.updateProperty("false");

        logFolder = initLoggers();

        doLogTest(logFolder, false);
    }

    @Test
    public void testLoggerAsyncWithCache() throws IOException {

        PAResourceManagerProperties.LOG4J_ASYNC_APPENDER_ENABLED.updateProperty("true");
        PAResourceManagerProperties.LOG4J_ASYNC_APPENDER_CACHE_ENABLED.updateProperty("true");

        File logFolder = initLoggers();

        doLogTest(logFolder, true);
    }

    private File initLoggers() throws IOException {
        Logger jobLogger = Logger.getLogger(JobLogger.class);
        FileAppender appender = new FileAppender();
        File logFolder = folder.newFolder("logs");
        appender.setFilesLocation(logFolder.getAbsolutePath());
        jobLogger.addAppender(appender);
        return logFolder;
    }

    @Test
    public void testLoggerSync() throws IOException {

        PAResourceManagerProperties.LOG4J_ASYNC_APPENDER_ENABLED.updateProperty("false");
        PAResourceManagerProperties.LOG4J_ASYNC_APPENDER_CACHE_ENABLED.updateProperty("false");

        File logFolder = initLoggers();

        doLogTest(logFolder, false);
    }

    private void doLogTest(File logFolder, boolean cacheEnabled) throws IOException {
        JobId id1 = new JobIdImpl(112, "readableName");
        JobId id2 = new JobIdImpl(113, "readableName");
        JobId id3 = new JobIdImpl(114, "readableName");
        JobLogger.getInstance().info(id1, "info message");
        JobLogger.getInstance().warn(id2, "warn message");
        JobLogger.getInstance().debug(id3, "debug message");
        Assert.assertEquals(cacheEnabled, FileAppender.doesCacheContain(JobLogger.getJobLogRelativePath(id1)));
        Assert.assertEquals(cacheEnabled, FileAppender.doesCacheContain(JobLogger.getJobLogRelativePath(id2)));
        Assert.assertEquals(cacheEnabled, FileAppender.doesCacheContain(JobLogger.getJobLogRelativePath(id3)));
        JobLogger.getInstance().close(id1);
        JobLogger.getInstance().close(id2);
        JobLogger.getInstance().close(id3);
        Assert.assertFalse(FileAppender.doesCacheContain(JobLogger.getJobLogRelativePath(id1)));
        Assert.assertFalse(FileAppender.doesCacheContain(JobLogger.getJobLogRelativePath(id2)));
        Assert.assertFalse(FileAppender.doesCacheContain(JobLogger.getJobLogRelativePath(id3)));
        Assert.assertEquals(1, Collections.list(Logger.getLogger(JobLogger.class).getAllAppenders()).size());
        String log1Content = IOUtils.toString(new File(logFolder, JobLogger.getJobLogRelativePath(id1)).toURI(),
                                              Charset.defaultCharset());
        String log2Content = IOUtils.toString(new File(logFolder, JobLogger.getJobLogRelativePath(id2)).toURI(),
                                              Charset.defaultCharset());
        String log3Content = IOUtils.toString(new File(logFolder, JobLogger.getJobLogRelativePath(id3)).toURI(),
                                              Charset.defaultCharset());
        System.out.println("------------------- log1 contents -----------------------");
        System.out.println(log1Content);
        System.out.println("------------------- log2 contents -----------------------");
        System.out.println(log2Content);
        System.out.println("------------------- log3 contents -----------------------");
        System.out.println(log3Content);
        Assert.assertThat(StringUtils.countMatches(log1Content, "info message"), is(1));
        Assert.assertThat(StringUtils.countMatches(log2Content, "warn message"), is(1));
        Assert.assertThat(StringUtils.countMatches(log3Content, "debug message"), is(1));
    }
}
