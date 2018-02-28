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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.log4j.Logger;
import org.objectweb.proactive.annotation.PublicAPI;
import org.ow2.proactive.resourcemanager.frontend.RMEventListener;
import org.ow2.proactive.resourcemanager.frontend.RMMonitoring;


/**
 * Defines a state of the Resource Manager for a Monitor.
 * In order to receive Resource Manager events,
 * a monitor register itself to {@link RMMonitoring} by
 * the method {@link RMMonitoring#addRMEventListener(RMEventListener listener, RMEventType... events)},
 * and get an initial state which is the snapshot of Resource Manager state, with its
 * nodes and NodeSources.
 *
 * @see RMNodeEvent
 * @see RMNodeSourceEvent
 * @see RMMonitoring
 *
 * @author The ProActive Team
 * @since ProActive Scheduling 0.9
 */
@PublicAPI
@XmlRootElement
public class RMInitialState implements Serializable {

    private static final Logger LOGGER = Logger.getLogger(RMInitialState.class);

    /**
     * Nodes events
     */
    private Map<String, RMNodeEvent> nodeEvents = new ConcurrentHashMap<>();

    /**
     * Nodes sources AO living in RM
     */
    private Map<String, RMNodeSourceEvent> nodeSourceEvents = new ConcurrentHashMap<>();

    private AtomicLong latestCounter = new AtomicLong(0);

    /**
     * ProActive empty constructor
     */
    public RMInitialState() {

    }

    /**
     * Creates an InitialState object.
     *
     * @param nodesEventList  RM's node events.
     * @param nodeSourcesList RM's node sources list.
     */
    public RMInitialState(Map<String, RMNodeEvent> nodesEventList, Map<String, RMNodeSourceEvent> nodeSourcesList) {
        this.nodeEvents = nodesEventList;
        this.nodeSourceEvents = nodeSourcesList;
    }

    /**
     * Current version of RM portal and maybe other clients expects "nodesEvents" inside JSON
     *
     * @return
     */
    public List<RMNodeEvent> getNodesEvents() {
        return Collections.unmodifiableList(new ArrayList<>(this.nodeEvents.values()));
    }

    /**
     * Current version of RM portal and maybe other clients expects "nodeSource" inside JSON
     *
     * @return
     */
    public List<RMNodeSourceEvent> getNodeSource() {
        return Collections.unmodifiableList(new ArrayList<>(this.nodeSourceEvents.values()));
    }

    public long getLatestCounter() {
        return latestCounter.get();
    }

    public void nodeAdded(RMNodeEvent event) {
        updateCounter(event);
        nodeEvents.put(event.getNodeUrl(), event);

    }

    public void nodeStateChanged(RMNodeEvent event) {
        updateCounter(event);
        nodeEvents.put(event.getNodeUrl(), event);
    }

    public void nodeRemoved(RMNodeEvent event) {
        updateCounter(event);
        nodeEvents.put(event.getNodeUrl(), event);
    }

    public void nodeSourceAdded(RMNodeSourceEvent event) {
        updateCounter(event);
        nodeSourceEvents.put(event.getSourceName(), event);
    }

    public void nodeSourceRemoved(RMNodeSourceEvent event) {
        updateCounter(event);
        nodeSourceEvents.put(event.getSourceName(), event);
    }

    private void updateCounter(RMEvent event) {
        latestCounter.set(Math.max(latestCounter.get(), event.getCounter()));
    }

    public RMInitialState cloneAndFilter(long filter) {
        long actualFilter;
        if (filter <= latestCounter.get()) {
            actualFilter = filter;
        } else {
            LOGGER.info(String.format("Client is aware of %d but server knows only about %d counter. " +
                                      "Probably because there was network server restart.",
                                      filter,
                                      latestCounter.get()));
            actualFilter = -1;
        }
        RMInitialState clone = new RMInitialState();

        clone.nodeEvents = newFilteredEvents(this.nodeEvents, actualFilter);
        clone.nodeSourceEvents = newFilteredEvents(this.nodeSourceEvents, actualFilter);

        clone.latestCounter.set(Math.max(actualFilter,
                                         Math.max(findLargestCounter(clone.nodeEvents.values()),
                                                  findLargestCounter(clone.nodeSourceEvents.values()))));
        return clone;
    }

    private <T extends RMEvent> Map<String, T> newFilteredEvents(Map<String, T> events, long filter) {
        Map<String, T> result = new ConcurrentHashMap<>();
        for (Map.Entry<String, T> entry : events.entrySet()) {
            if (entry.getValue().getCounter() > filter) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    private <T extends RMEvent> long findLargestCounter(Collection<T> events) {
        long result = 0;
        for (T event : events) {
            if (result < event.getCounter()) {
                result = event.getCounter();
            }
        }
        return result;
    }

    public void nodeSourceStateChanged(RMNodeSourceEvent stateChangedEvent) {
        boolean existNodeSource = false;
        int size = nodeSourceEvents.size();
        for (int i = 0; i < size; i++) {
            if (stateChangedEvent.getSourceName().equals(nodeSourceEvents.get(i).getSourceName())) {
                existNodeSource = true;
                nodeSourceEvents.put(stateChangedEvent.getSourceName(), stateChangedEvent);
                break;
            }
        }
        if (!existNodeSource) {
            nodeSourceEvents.put(stateChangedEvent.getSourceName(), stateChangedEvent);
        }
    }
}
