/*
 * ################################################################
 *
 * ProActive Parallel Suite(TM): The Java(TM) library for
 *    Parallel, Distributed, Multi-Core Computing for
 *    Enterprise Grids & Clouds
 *
 * Copyright (C) 1997-2011 INRIA/University of
 *                 Nice-Sophia Antipolis/ActiveEon
 * Contact: proactive@ow2.org or contact@activeeon.com
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Affero General Public License
 * as published by the Free Software Foundation; version 3 of
 * the License.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
 * USA
 *
 * If needed, contact us to obtain a release under GPL Version 2 or 3
 * or a different license than the AGPL.
 *
 *  Initial developer(s):               The ProActive Team
 *                        http://proactive.inria.fr/team_members.htm
 *  Contributor(s): ActiveEon Team - http://www.activeeon.com
 *
 * ################################################################
 * $$ACTIVEEON_CONTRIBUTOR$$
 */
package functionaltests.executables;

import org.objectweb.proactive.extensions.dataspaces.api.FileSelector;
import org.ow2.proactive.scheduler.common.task.TaskResult;
import org.ow2.proactive.scheduler.common.task.executable.JavaExecutable;

import java.io.Serializable;


public class DSWorker extends JavaExecutable {

    //set automatically by java executable
    private String paa;
    private String pbb;
    private String pcc;
    private String paaa;
    private String pbbb;
    private String pccc;

    @Override
    public Serializable execute(TaskResult... results) throws Throwable {
        getOutputFile(paaa).copyFrom(getOutputFile(paa), FileSelector.SELECT_SELF);
        getGlobalFile(pbbb).copyFrom(getGlobalFile(pbb), FileSelector.SELECT_SELF);
        getUserFile(pccc).copyFrom(getUserFile(pcc), FileSelector.SELECT_SELF);
        return 0;
    }

}
