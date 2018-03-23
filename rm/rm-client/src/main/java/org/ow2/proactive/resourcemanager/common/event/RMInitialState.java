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
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.objectweb.proactive.annotation.PublicAPI;
import org.ow2.proactive.resourcemanager.common.event.dto.RMStateDTO;
import org.ow2.proactive.resourcemanager.core.properties.PAResourceManagerProperties;
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
public class RMInitialState implements Serializable {

    public static final Long EMPTY_STATE = -1L;

    private static final Logger LOGGER = Logger.getLogger(RMInitialState.class);

    private SortedUniqueSet<RMEvent> events = new SortedUniqueSet<>();

    /**
     * keeps track of the latest (biggest) counter among the 'nodeEvents' and 'nodeSourceEvents'
     */
    private AtomicLong latestCounter = new AtomicLong(0);

    /**
     * ProActive empty constructor
     */
    public RMInitialState() {

    }

    public void addAll(Collection<? extends RMEvent> toAdd) {
        for (RMEvent rmEvent : toAdd) {
            events.add(rmEvent);
        }
        latestCounter.set(Math.max(latestCounter.get(), findLargestCounter(events.getSortedItems())));
    }

    /**
     * Current version of RM portal and maybe other clients expects "nodesEvents" inside JSON
     *
     * @return list of RMNodeEvent
     */
    public List<RMNodeEvent> getNodesEvents() {
        return getNodesEvents(events.getSortedItems());
    }

    /**
     * Current version of RM portal and maybe other clients expects "nodeSource" inside JSON
     *
     * @return list of RMNodeSourceEvent
     */
    public List<RMNodeSourceEvent> getNodeSource() {
        return getNodeSource(events.getSortedItems());
    }

    public List<RMNodeEvent> getNodesEvents(Collection<RMEvent> rmEvents) {
        return rmEvents.stream()
                .filter(event -> event instanceof RMNodeEvent)
                .map(event -> (RMNodeEvent) event)
                .collect(Collectors.toList());
    }

    public List<RMNodeSourceEvent> getNodeSource(Collection<RMEvent> rmevents) {
        return rmevents.stream()
                       .filter(event -> event instanceof RMNodeSourceEvent)
                       .map(event -> (RMNodeSourceEvent) event)
                       .collect(Collectors.toList());
    }

    public long getLatestCounter() {
        return latestCounter.get();
    }

    public void nodeAdded(RMNodeEvent event) {
        update(event);
    }

    public void nodeStateChanged(RMNodeEvent event) {
        update(event);
    }

    public void nodeRemoved(RMNodeEvent event) {
        update(event);
    }

    public void nodeSourceAdded(RMNodeSourceEvent event) {
        update(event);
    }

    public void nodeSourceRemoved(RMNodeSourceEvent event) {
        update(event);
    }

    public void nodeSourceStateChanged(RMNodeSourceEvent event) {
        update(event);
    }

    protected void update(RMEvent event) {
        events.add(event);
        updateCounter(event);
    }

    /**
     * Clones current state events, but keep only those events which has counter bigger than provided 'filter'
     * Event counter can take values [0, +).
     * So if filter is '-1' then all events will returned.
     * @param filter
     * @return rmInitialState where all the events bigger than 'filter'
     */
    public RMStateDTO cloneAndFilter(long filter) {
        long actualFilter = computeActualFilter(filter);

        final List<RMEvent> responseEvents = events.getSortedItems()
                                                   .tailSet(new RMEvent(actualFilter + 1)) // because tailSet returns event which is equal or greater
                                                   .stream()
                                                   .limit(PAResourceManagerProperties.RM_REST_MONITORING_MAXIMUM_CHUNK_SIZE.getValueAsInt())
                                                   .collect(Collectors.toList());

        RMStateDTO response = new RMStateDTO();

        response.setNodeSource(getNodeSource(responseEvents));
        response.setNodesEvents(getNodesEvents(responseEvents));
        response.setLatestCounter(Math.max(actualFilter, findLargestCounter(responseEvents)));

        return response;
    }

    private long computeActualFilter(long clientFilter) {
        if (clientFilter <= latestCounter.get()) {
            return clientFilter;
        } else {
            LOGGER.info(String.format("Client is aware of %d but server knows only about %d counter. " +
                                      "Probably because there was network server restart.",
                                      clientFilter,
                                      latestCounter.get()));
            return EMPTY_STATE; // reset filter to default  value
        }
    }

    private void updateCounter(RMEvent event) {
        latestCounter.set(Math.max(latestCounter.get(), event.getCounter()));
    }

    private <T extends RMEvent> long findLargestCounter(Collection<T> events) {
        final Optional<T> max = events.stream().max(Comparator.comparing(RMEvent::getCounter));
        return max.map(RMEvent::getCounter).orElse(0l);
    }
}
