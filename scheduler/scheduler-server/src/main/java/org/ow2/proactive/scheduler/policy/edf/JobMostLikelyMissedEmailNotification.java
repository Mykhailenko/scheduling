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
package org.ow2.proactive.scheduler.policy.edf;

import static org.ow2.proactive.scheduler.core.JobEmailNotification.GENERIC_INFORMATION_KEY_EMAIL;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;
import java.util.TimeZone;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.ow2.proactive.addons.email.exception.EmailException;
import org.ow2.proactive.scheduler.core.JobEmailNotificationException;
import org.ow2.proactive.scheduler.core.properties.PASchedulerProperties;
import org.ow2.proactive.scheduler.job.InternalJob;
import org.ow2.proactive.scheduler.util.JobLogger;
import org.ow2.proactive.scheduler.util.SendMail;


public class JobMostLikelyMissedEmailNotification {

    private static final Logger logger = Logger.getLogger(JobMostLikelyMissedEmailNotification.class);

    private static final JobLogger jlogger = JobLogger.getInstance();

    private final InternalJob internalJob;

    private final Date expectedFinishingTime;

    private final Date deadline;

    private Duration jobExecutionTime = null;

    public JobMostLikelyMissedEmailNotification(InternalJob internalJob, Date expectedFinishingTime, Date deadline) {
        this.internalJob = internalJob;
        this.expectedFinishingTime = expectedFinishingTime;
        this.deadline = deadline;
    }

    public JobMostLikelyMissedEmailNotification(InternalJob internalJob, Date expectedFinishingTime, Date deadline,
            Duration jobExecutionTime) {
        this.internalJob = internalJob;
        this.expectedFinishingTime = expectedFinishingTime;
        this.deadline = deadline;
        this.jobExecutionTime = jobExecutionTime;
    }

    public static void main(String[] args) {
        System.out.println(new Date().getTime());
        System.out.println(System.currentTimeMillis());
    }

    public boolean doSend() throws JobEmailNotificationException {
        if (!PASchedulerProperties.EMAIL_NOTIFICATIONS_ENABLED.getValueAsBoolean()) {
            logger.debug("Notification emails disabled, doing nothing");
            return false;
        }
        try {
            new SendMail().sender(getTo(), getSubject(), getBody());
            return true;
        } catch (EmailException | IOException e) {
            throw new JobEmailNotificationException("Error sending email: " + e.getMessage(), e);
        }
    }

    public void checkAndSend() {
        try {
            boolean sent = doSend();
            if (sent) {
                jlogger.info(internalJob.getId(), "sent notification email for finished job");
            }
        } catch (JobEmailNotificationException e) {
            jlogger.warn(internalJob.getId(), "failed to send email notification: " + e.getMessage());
            logger.trace("Stack trace:", e);
        }
    }

    private String getTo() throws JobEmailNotificationException {
        String to = internalJob.getJobInfo().getGenericInformation().get(GENERIC_INFORMATION_KEY_EMAIL);
        if (to == null) {
            throw new JobEmailNotificationException("Recipient address is not set in generic information");
        }
        return to;
    }

    private String getSubject() {
        return String.format("WARNING: ProActive Job %s most likely will miss its deadline",
                             internalJob.getId().value());
    }

    private static final SimpleDateFormat jobExecutionTimeFormat;

    static {
        jobExecutionTimeFormat = new SimpleDateFormat("HH:mm:ss");
        jobExecutionTimeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    }

    private String getBody() throws IOException {
        final ClassLoader classLoader = JobMostLikelyMissedEmailNotification.class.getClassLoader();
        if (jobExecutionTime != null) {
            final String template = IOUtils.toString(Objects.requireNonNull(classLoader.getResource("email-templates/most-likely-missed-with-duration.template")),
                                                     Charset.defaultCharset());

            Calendar calendar = Calendar.getInstance();
            calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
            calendar.setTimeInMillis(0);
            calendar.add(Calendar.SECOND, (int) jobExecutionTime.getSeconds());
            final String strJobExecutionTime = jobExecutionTimeFormat.format(calendar.getTime());

            return String.format(template,
                                 internalJob.getId().value(),
                                 deadline,
                                 strJobExecutionTime,
                                 expectedFinishingTime);
        } else {
            final String template = IOUtils.toString(Objects.requireNonNull(classLoader.getResource("email-templates/most-likely-missed-without-duration.template")),
                                                     Charset.defaultCharset());

            return String.format(template, internalJob.getId().value(), deadline, expectedFinishingTime);
        }
    }
}
