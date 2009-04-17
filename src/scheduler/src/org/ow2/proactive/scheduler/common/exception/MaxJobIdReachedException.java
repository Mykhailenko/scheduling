/*
 * ################################################################
 *
 * ProActive: The Java(TM) library for Parallel, Distributed,
 *            Concurrent computing with Security and Mobility
 *
 * Copyright (C) 1997-2008 INRIA/University of Nice-Sophia Antipolis
 * Contact: proactive@ow2.org
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version
 * 2 of the License, or any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
 * USA
 *
 *  Initial developer(s):               The ProActive Team
 *                        http://proactive.inria.fr/team_members.htm
 *  Contributor(s):
 *
 * ################################################################
 * $PROACTIVE_INITIAL_DEV$
 */
package org.ow2.proactive.scheduler.common.exception;

/**
 * Exception generated by the scheduler JobId nextId() method informing that max Id has been reached.<br>
 * In this case, the Scheduler is stopped, and administrator may check what to do with the Database.
 *
 * @author The ProActive Team
 * @since ProActive Scheduling 1.0
 */
public class MaxJobIdReachedException extends SchedulerException {
    /**
     * Create a new instance of MaxJobIdReachedException with the given message.
     *
     * @param msg the message to attach.
     */
    public MaxJobIdReachedException(String msg) {
        super(msg);
    }

    /**
     * Create a new instance of MaxJobIdReachedException.
     */
    public MaxJobIdReachedException() {
        super();
    }

    /**
     * Create a new instance of MaxJobIdReachedException with the given message and cause
     *
     * @param msg the message to attach.
     * @param cause the cause of the exception.
     */
    public MaxJobIdReachedException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Create a new instance of MaxJobIdReachedException with the given cause.
     *
     * @param cause the cause of the exception.
     */
    public MaxJobIdReachedException(Throwable cause) {
        super(cause);
    }
}
