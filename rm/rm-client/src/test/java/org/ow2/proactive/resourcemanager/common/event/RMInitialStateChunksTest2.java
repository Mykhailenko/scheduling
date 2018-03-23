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
package org.ow2.proactive.resourcemanager.common.event;

import static org.junit.Assert.assertEquals;
import static org.ow2.proactive.resourcemanager.core.properties.PAResourceManagerProperties.RM_REST_MONITORING_MAXIMUM_CHUNK_SIZE;

import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;
import org.ow2.proactive.resourcemanager.common.event.dto.RMStateDTO;


public class RMInitialStateChunksTest2 {

    private RMInitialState rmInitialState;

    private long counter = 0;

    @Before
    public void init() {
        rmInitialState = new RMInitialState();

        RM_REST_MONITORING_MAXIMUM_CHUNK_SIZE.updateProperty("50");

        IntStream.range(0, 4).forEach(nodeSourceId -> {
            rmInitialState.nodeSourceAdded(new RMNodeSourceEvent("LocalNodes", counter++));
            rmInitialState.nodeAdded(new RMNodeEvent("pnp...", counter++));
        });

    }

    @Test
    public void clientKnowsNothing() {
        assertEquals(1, rmInitialState.cloneAndFilter(RMInitialState.EMPTY_STATE).getNodeSource().size());
        assertEquals(1, rmInitialState.cloneAndFilter(RMInitialState.EMPTY_STATE).getNodesEvents().size());
    }

}
