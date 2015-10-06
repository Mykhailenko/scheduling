/*
 * ################################################################
 *
 * ProActive Parallel Suite(TM): The Java(TM) library for
 *    Parallel, Distributed, Multi-Core Computing for
 *    Enterprise Grids & Clouds
 *
 * Copyright (C) 1997-2014 INRIA/University of
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
 *  Contributor(s):
 *
 * ################################################################
 * $$ACTIVEEON_INITIAL_DEV$$
 */

package org.ow2.proactive_grid_cloud_portal.cli.cmd;

import java.io.PrintWriter;
import java.io.Writer;

import org.ow2.proactive_grid_cloud_portal.cli.ApplicationContext;
import org.ow2.proactive_grid_cloud_portal.cli.CLIException;
import org.ow2.proactive_grid_cloud_portal.cli.CommandFactory;
import org.ow2.proactive_grid_cloud_portal.cli.utils.ProActiveVersionUtility;
import org.apache.commons.cli.HelpFormatter;


public class HelpCommand extends AbstractCommand implements Command {

    public static final String USAGE = "proactive-client [OPTIONS]";

    public HelpCommand() {
    }

    @Override
    public void execute(ApplicationContext currentContext) throws CLIException {
        HelpFormatter formatter = new HelpFormatter();
        Writer writer = currentContext.getDevice().getWriter();

        ProActiveVersionUtility.writeProActiveVersionWithBreakEndLine(currentContext, System.out);

        PrintWriter pw = new PrintWriter(writer, true);
        formatter.printHelp(pw, 110, USAGE, "", CommandFactory.getCommandFactory(CommandFactory.Type.ALL)
                .supportedOptions(), formatter.getLeftPadding(), formatter.getDescPadding(), "", false);
    }

}
