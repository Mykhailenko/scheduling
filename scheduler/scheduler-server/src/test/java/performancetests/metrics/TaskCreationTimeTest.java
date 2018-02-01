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
package performancetests.metrics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;

import java.util.Arrays;
import java.util.Collection;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.objectweb.proactive.core.config.ProActiveConfiguration;
import org.ow2.proactive.resourcemanager.RMFactory;
import org.ow2.proactive.scheduler.common.job.JobId;

import functionaltests.utils.SchedulerTHelper;
import performancetests.recovery.PeformanceTestBase;


@RunWith(Parameterized.class)
public class TaskCreationTimeTest extends PeformanceTestBase {

    private static final Logger LOGGER = Logger.getLogger(TaskCreationTimeTest.class);

    private static final int TASK_DURATION = 10; // in seconds

    /**
     * @return an array of parameters which is used by JUnit to create objects of TaskCreationTime,
     * where first value represents number of task in the job, and the second represents limit for TaskCreationTime (TCT).
     * The lower TCT the better.
     */
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { { 20000, 15000 } });
    }

    private final int taskNumber;

    private final long timeLimit;

    public TaskCreationTimeTest(int taskNumber, long timeLimit) {
        this.taskNumber = taskNumber;
        this.timeLimit = timeLimit;
    }

    @Test(timeout = 3600000)
    public void taskCreationRate() throws Exception {
        ProActiveConfiguration.load();
        RMFactory.setOsJavaProperty();
        schedulerHelper = new SchedulerTHelper(false,
                                               SchedulerEfficiencyTimeTest.SCHEDULER_CONFIGURATION_START.getPath(),
                                               SchedulerEfficiencyTimeTest.RM_CONFIGURATION_START.getPath(),
                                               null);

        schedulerHelper.createNodeSourceWithInfiniteTimeout("local", 1);

        final long start = System.currentTimeMillis();

        final JobId jobId = schedulerHelper.submitJob(SchedulerEfficiencyTimeTest.createJob(taskNumber, TASK_DURATION));

        final long anActualTime = System.currentTimeMillis() - start;

        LOGGER.info(makeCSVString(TaskCreationTimeTest.class.getSimpleName(),
                                  taskNumber,
                                  timeLimit,
                                  anActualTime,
                                  ((anActualTime < timeLimit) ? SUCCESS : FAILURE)));

        assertThat(String.format("Task creation rate for job with %s tasks", taskNumber),
                   anActualTime,
                   lessThan(timeLimit));

    }

}
