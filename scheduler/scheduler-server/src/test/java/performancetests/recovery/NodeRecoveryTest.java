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
package performancetests.recovery;

import functionaltests.utils.TestScheduler;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.threads.JMeterContextService;

import org.junit.Test;
import performancetests.recovery.helper.NodeRecoveryHelper;

import static org.junit.Assert.assertTrue;


public class NodeRecoveryTest extends AbstractJavaSamplerClient {

    @Test
    public void test() {
        System.out.println(TestScheduler.testClasspath());
//        SampleResult sampleResult = new NodeRecoveryTest().runTestHelped(10);
//        assertTrue(sampleResult.isSuccessful());
    }

    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {
        final Integer nodesNumber = Integer.valueOf(JMeterContextService.getContext()
                .getVariables()
                .get("nodesNumber"));
        return runTestHelped(nodesNumber);
    }


    public SampleResult runTestHelped(int nodesNumber) {
        SampleResult sampleResult = null;
        try {
            NodeRecoveryHelper nodeRecoveryHelper = new NodeRecoveryHelper();
            final long initalTime = System.currentTimeMillis();

            nodeRecoveryHelper.startKillStartScheduler(nodesNumber);

            sampleResult = SampleResult.createTestSample(nodeRecoveryHelper.timeSpentToRecoverNodes(initalTime));
            sampleResult.setResponseCodeOK();
            sampleResult.setSuccessful(true);
            sampleResult.setResponseMessage("I'm Forever Blowing Bubbles");
            nodeRecoveryHelper.shutdown();

        } catch (Exception e) {
            System.out.println("\t\t\t\t\t\t");
            e.printStackTrace();
        }
        return sampleResult;
    }
}
