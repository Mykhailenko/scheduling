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

import static java.util.Arrays.asList;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.ow2.proactive.scheduler.common.JobDescriptor;
import org.ow2.proactive.scheduler.common.job.JobPriority;
import org.ow2.proactive.scheduler.common.job.JobState;
import org.ow2.proactive.scheduler.core.JobEmailNotificationException;
import org.ow2.proactive.scheduler.descriptor.EligibleTaskDescriptor;
import org.ow2.proactive.scheduler.descriptor.JobDescriptorImpl;
import org.ow2.proactive.scheduler.job.InternalJob;
import org.ow2.proactive.scheduler.policy.ExtendedSchedulerPolicy;


/**
 * Early Deadline First Policy sorts jobs based on:
 * - job priorities (from highest to lowest)
 * - job deadline (those that have deadline overtake those without deadline)
 * <p>
 * Among the job with the dealine (and having same priority) we distinguish
 * them by the fact if some task of the job is already scheduled (i.g. job has
 * a startingTime) or not. Those that started has a priority to the not yet started jobs.
 * Among the jobs that are already started we sort them by startTime.
 * If job is not started, we sort them by `effectiveDeadline - (now() + expectedTime`.
 * If deadline is absolute then effectiveDeadline equals to deadline.
 * If deadline is relative then effectiveDeadline is calculated as (now() + deadline)
 * <p>
 * Among jobs without deadline (and the same priority), we sort them by submission date.
 *
 * @author The ProActive Team
 * @since ProActive Scheduling 8.3
 */
public class EDFPolicyExtended extends ExtendedSchedulerPolicy {

    private static final Date MAXIMUM_DATE = new Date(Long.MAX_VALUE);

    private static final Logger LOGGER = Logger.getLogger(EDFPolicyExtended.class);

    @Override
    public LinkedList<EligibleTaskDescriptor> getOrderedTasks(List<JobDescriptor> jobs) {
        fireEventsIfSomeJobsWillNotMeetTheirDeadlines(jobs);

        return jobs.stream()
                   .sorted(new CompositeComparator(byPriority,
                                                   deadlineOverNoDeadline,
                                                   startedOverNotStarted,
                                                   byStartedTime,
                                                   byDistanceBetweenFinishAndDeadline.apply(new Date()),
                                                   bySubmittedTime))
                   .map(JobDescriptor::getEligibleTasks)
                   .flatMap(Collection::stream)
                   .map(taskDescriptors -> (EligibleTaskDescriptor) taskDescriptors)
                   .collect(Collectors.toCollection(LinkedList::new));
    }

    private void fireEventsIfSomeJobsWillNotMeetTheirDeadlines(List<JobDescriptor> jobs) {
        Date now = new Date();
        final List<InternalJob> jobsInDanger = jobs.stream()
                                                   .map(jobDescriptor -> ((JobDescriptorImpl) jobDescriptor).getInternal())
                                                   .filter(job -> {
                                                       Date effectiveDeadline = getEffectiveDeadline(job, now);
                                                       Date effectiveExpExecTime = getEffectiveExpectedExecutionTime(job,
                                                                                                                     now);
                                                       return effectiveDeadline.compareTo(effectiveExpExecTime) < 0;
                                                   })
                                                   .collect(Collectors.toList());

        jobsInDanger.forEach(job -> {
            LOGGER.warn(String.format("Job[id=%s] might miss its deadline (expected finish: %s after deadline: %s)",
                                      job.getId().value(),
                                      getEffectiveExpectedExecutionTime(job, now),
                                      getEffectiveDeadline(job, now)));
            try {
                new JobDeadlineEmailNotification(job).doSend();
            } catch (JobEmailNotificationException e) {
                LOGGER.error("Cannot send an email: ", e);
            }
        });
    }

    private static Duration durationBetweenFinishAndDeadline(InternalJob internalJob, Date now) {
        if (internalJob.getJobDeadline().isPresent()) {
            final Date effectiveDeadline = getEffectiveDeadline(internalJob, now);
            final Date effectiveExpectedExecutionTime = getEffectiveExpectedExecutionTime(internalJob, now);
            final long gapInMillis = effectiveDeadline.getTime() - effectiveExpectedExecutionTime.getTime();
            return Duration.ofMillis(gapInMillis);
        } else {
            return Duration.ZERO;
        }
    }

    /**
     * @return deadline of the job if existed, otherwise returns biggest date possible
     * (this is how this policy treats absence of deadline)
     */
    public static Date getEffectiveDeadline(InternalJob internalJob, Date now) {
        if (internalJob.getJobDeadline().isPresent()) {
            if (internalJob.getJobDeadline().get().isAbsolute()) {
                return internalJob.getJobDeadline().get().getAbsoluteDeadline();
            } else {
                final Duration relativeDeadline = internalJob.getJobDeadline().get().getRelativeDeadline();
                Calendar cal = Calendar.getInstance(); // creates calendar
                cal.setTime(now); // sets calendar time/date
                cal.add(Calendar.SECOND, (int) relativeDeadline.getSeconds()); // adds one hour
                return cal.getTime();
            }
        } else {
            return MAXIMUM_DATE;
        }
    }

    public static Date getEffectiveExpectedExecutionTime(InternalJob internalJob, Date now) {
        if (internalJob.getJobExpectedExecutionTime().isPresent()) {
            final Duration expectedTime = internalJob.getJobExpectedExecutionTime().get();
            Calendar cal = Calendar.getInstance(); // creates calendar
            cal.setTime(now); // sets calendar time/date
            cal.add(Calendar.SECOND, (int) expectedTime.getSeconds()); // adds one hour
            return cal.getTime();
        } else {
            return now;
        }
    }

    private static class CompositeComparator implements Comparator<JobDescriptor> {

        private List<Comparator<InternalJob>> comparators;

        public CompositeComparator(Comparator<InternalJob>... comparators) {
            this.comparators = asList(comparators);
        }

        @Override
        public int compare(JobDescriptor job1, JobDescriptor job2) {
            InternalJob internal1 = ((JobDescriptorImpl) job1).getInternal();
            InternalJob internal2 = ((JobDescriptorImpl) job2).getInternal();

            for (Comparator<InternalJob> comparator : comparators) {
                final int result = comparator.compare(internal1, internal2);
                if (result != 0) {
                    return result;
                }
            }
            return 0;
        }
    }

    private static final Comparator<InternalJob> byPriority = Comparator.comparing(JobState::getPriority);

    private static final Comparator<InternalJob> deadlineOverNoDeadline = (job1, job2) -> {
        if (job1.getJobDeadline().isPresent() && !job2.getJobDeadline().isPresent()) {
            return -1;
        } else if (!job1.getJobDeadline().isPresent() && job2.getJobDeadline().isPresent()) {
            return 1;
        } else {
            return 0;
        }
    };

    private static final Comparator<InternalJob> startedOverNotStarted = (job1, job2) -> {
        if (job1.getJobInfo().getStartTime() >= 0 && job2.getJobInfo().getStartTime() < 0) {
            return -1;
        } else if (job1.getJobInfo().getStartTime() < 0 && job2.getJobInfo().getStartTime() >= 0) {
            return 1;
        } else {
            return 0;
        }
    };

    private static final Comparator<InternalJob> byStartedTime = (job1, job2) -> {
        if (job1.getJobInfo().getStartTime() >= 0 && job2.getJobInfo().getStartTime() >= 0) {
            return Long.compare(job1.getJobInfo().getStartTime(), job2.getJobInfo().getStartTime());
        } else {
            return 0;
        }
    };

    private static final Function<Date, Comparator<InternalJob>> byDistanceBetweenFinishAndDeadline = (
            now) -> (job1, job2) -> {
                Duration gap1 = durationBetweenFinishAndDeadline(job1, now);
                Duration gap2 = durationBetweenFinishAndDeadline(job2, now);
                return gap1.compareTo(gap2);
            };

    private static final Comparator<InternalJob> bySubmittedTime = (job1,
            job2) -> Long.compare(job1.getJobInfo().getSubmittedTime(), job1.getJobInfo().getSubmittedTime());

}
