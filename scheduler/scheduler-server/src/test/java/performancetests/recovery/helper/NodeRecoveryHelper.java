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
package performancetests.recovery.helper;

import java.io.File;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import functionaltests.recover.TaskReconnectionToRecoveredNodeTest;
import functionaltests.utils.*;
import org.apache.log4j.Appender;
import org.apache.log4j.Logger;
import org.codehaus.groovy.runtime.powerassert.SourceText;
import org.grep4j.core.model.Profile;
import org.grep4j.core.model.ProfileBuilder;
import org.grep4j.core.result.GrepResults;
import org.ow2.proactive.resourcemanager.RMFactory;
import org.ow2.proactive.resourcemanager.common.RMConstants;
import org.ow2.proactive.resourcemanager.core.RMCore;
import org.ow2.proactive.resourcemanager.frontend.ResourceManager;

import functionaltests.recover.TaskReconnectionWithForkedTaskExecutorTest;

import static org.grep4j.core.Grep4j.constantExpression;
import static org.grep4j.core.Grep4j.grep;
import static org.grep4j.core.fluent.Dictionary.on;
import static org.junit.Assert.assertEquals;


public class NodeRecoveryHelper extends SchedulerFunctionalTestWithCustomConfigAndRestart {

//    private static final URL SCHEDULER_CONFIGURATION_START = NodeRecoveryHelper.class.getResource("/performancetests/config/scheduler-config-start.ini");
    private static final URL SCHEDULER_CONFIGURATION_START = NodeRecoveryHelper.class.getResource("/functionaltests/config/functionalTSchedulerProperties.ini");

    private static final URL SCHEDULER_CONFIGURATION_RESTART = TaskReconnectionWithForkedTaskExecutorTest.class.getResource("/performancetests/config/scheduler-config-restart.ini");

//    private static final URL RM_CONFIGURATION_START = NodeRecoveryHelper.class.getResource("/performancetests/config/rm-config-start.ini");
    private static final URL RM_CONFIGURATION_START = NodeRecoveryHelper.class.getResource("/functionaltests/config/functionalTRMProperties-clean-db.ini");

    private static final URL RM_CONFIGURATION_RESTART = TaskReconnectionToRecoveredNodeTest.class.getResource("/performancetests/config/rm-config-restart.ini");

    private SchedulerTHelper schedulerHelper;

    private List<TestNode> nodes;

    public void startKillStartScheduler(int nodesNumber) throws Exception {
        RMFactory.setOsJavaProperty();
        schedulerHelper = new SchedulerTHelper(false,
                new File(SCHEDULER_CONFIGURATION_START.toURI()).getAbsolutePath(),
                new File(RM_CONFIGURATION_START.toURI()).getAbsolutePath(),
                null);

        // start nodes
        ResourceManager rm = schedulerHelper.getResourceManager();

        RMTHelper rmHelper = new RMTHelper();

        nodes = schedulerHelper.createRMNodeStarterNodes(NodeRecoveryHelper.class.getSimpleName(),
                nodesNumber);

        rmHelper.waitForNodeSourceCreation(RMConstants.DEFAULT_STATIC_SOURCE_NAME);
        // kill server
        TestScheduler.kill();

        schedulerHelper = new SchedulerTHelper(false,
                new File(SCHEDULER_CONFIGURATION_RESTART.toURI()).getAbsolutePath(),
                new File(RM_CONFIGURATION_RESTART.toURI()).getAbsolutePath(),
                null);
    }

    public long timeSpentToRecoverNodes(long initalTime) {
        Profile localProfile = ProfileBuilder.newBuilder()
                .name("Local server log" + System.currentTimeMillis())
                .filePath(System.getProperty("pa.rm.home") + "/logs/Scheduler-tests.log").onLocalhost().build();

        GrepResults startedResults = grep(constantExpression(RMCore.RMCORE_RESTORE_NODES_STARTED), on(localProfile));
        GrepResults finishedResults = grep(constantExpression(RMCore.RMCORE_RESTORE_NODES_FINISHED), on(localProfile));

        assertEquals(1, startedResults.totalLines());
        assertEquals(1, finishedResults.totalLines());

        final String startedDateString = startedResults.iterator().next().getText().substring(1,  "[2017-11-11 11:11:11,111 ".length());
        final String finishedDateString = finishedResults.iterator().next().getText().substring(1, "[2017-11-11 11:11:11,111 ".length());
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");

        Date startedDate;
        Date finishedDate;
        try {
            startedDate = dateFormat.parse(startedDateString);
            finishedDate = dateFormat.parse(finishedDateString);
        } catch (ParseException e) {
            throw new RuntimeException("Cannot parse \"" + startedDateString + "\" to date.", e);
        }
        long startedMilliseconds;
        if(initalTime < startedDate.getTime()){
            startedMilliseconds = startedDate.getTime();
        }else{
            throw new RuntimeException("There is no \"" + RMCore.RMCORE_RESTORE_NODES_STARTED + "\" line AFTER we startedMilliseconds our Scheduler.");
        }
        long finishedMilliseconds;
        if(initalTime < finishedDate.getTime()){
            finishedMilliseconds = finishedDate.getTime();
        }else{
            throw new RuntimeException("There is no \"" + RMCore.RMCORE_RESTORE_NODES_FINISHED + "\" line AFTER we startedMilliseconds our Scheduler.");
        }

        if(finishedMilliseconds >= startedMilliseconds){
            return finishedMilliseconds - startedMilliseconds;
        }else{
            throw new RuntimeException("Timestamp finishedMilliseconds before it was startedMilliseconds");
        }
    }

    public void shutdown() throws Exception {
        if (nodes != null) {
            for (TestNode node : nodes) {
                node.kill();
            }
        }
        schedulerHelper.killScheduler();
        cleanupScheduler();

    }

}
