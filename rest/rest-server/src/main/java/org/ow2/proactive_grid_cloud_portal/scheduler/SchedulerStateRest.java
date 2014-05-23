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
 *  Initial developer(s):               The ActiveEon Team
 *                        http://www.activeeon.com/
 *  Contributor(s):
 *
 * ################################################################
 * $$ACTIVEEON_INITIAL_DEV$$
 */
package org.ow2.proactive_grid_cloud_portal.scheduler;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.SequenceInputStream;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.KeyException;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.security.auth.login.LoginException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.objectweb.proactive.ActiveObjectCreationException;
import org.objectweb.proactive.api.PAFuture;
import org.objectweb.proactive.core.node.NodeException;
import org.objectweb.proactive.core.util.log.ProActiveLogger;
import org.objectweb.proactive.extensions.dataspaces.vfs.VFSFactory;
import org.objectweb.proactive.utils.StackTraceUtil;
import org.ow2.proactive.authentication.crypto.CredData;
import org.ow2.proactive.authentication.crypto.Credentials;
import org.ow2.proactive.db.SortOrder;
import org.ow2.proactive.db.SortParameter;
import org.ow2.proactive.scheduler.common.JobFilterCriteria;
import org.ow2.proactive.scheduler.common.JobSortParameter;
import org.ow2.proactive.scheduler.common.Scheduler;
import org.ow2.proactive.scheduler.common.SchedulerAuthenticationInterface;
import org.ow2.proactive.scheduler.common.SchedulerConnection;
import org.ow2.proactive.scheduler.common.SchedulerConstants;
import org.ow2.proactive.scheduler.common.exception.ConnectionException;
import org.ow2.proactive.scheduler.common.exception.InternalSchedulerException;
import org.ow2.proactive.scheduler.common.exception.JobAlreadyFinishedException;
import org.ow2.proactive.scheduler.common.exception.JobCreationException;
import org.ow2.proactive.scheduler.common.exception.NotConnectedException;
import org.ow2.proactive.scheduler.common.exception.PermissionException;
import org.ow2.proactive.scheduler.common.exception.SchedulerException;
import org.ow2.proactive.scheduler.common.exception.SubmissionClosedException;
import org.ow2.proactive.scheduler.common.exception.UnknownJobException;
import org.ow2.proactive.scheduler.common.exception.UnknownTaskException;
import org.ow2.proactive.scheduler.common.job.Job;
import org.ow2.proactive.scheduler.common.job.JobId;
import org.ow2.proactive.scheduler.common.job.JobInfo;
import org.ow2.proactive.scheduler.common.job.JobPriority;
import org.ow2.proactive.scheduler.common.job.JobResult;
import org.ow2.proactive.scheduler.common.job.JobState;
import org.ow2.proactive.scheduler.common.job.factories.FlatJobFactory;
import org.ow2.proactive.scheduler.common.job.factories.JobFactory;
import org.ow2.proactive.scheduler.common.task.TaskResult;
import org.ow2.proactive.scheduler.common.task.TaskState;
import org.ow2.proactive.scheduler.common.util.SchedulerProxyUserInterface;
import org.ow2.proactive.scheduler.common.util.logforwarder.LogForwardingException;
import org.ow2.proactive_grid_cloud_portal.common.SchedulerRestInterface;
import org.ow2.proactive_grid_cloud_portal.common.Session;
import org.ow2.proactive_grid_cloud_portal.common.SessionStore;
import org.ow2.proactive_grid_cloud_portal.common.SharedSessionStore;
import org.ow2.proactive_grid_cloud_portal.common.dto.LoginForm;
import org.ow2.proactive_grid_cloud_portal.scheduler.dto.JobIdData;
import org.ow2.proactive_grid_cloud_portal.scheduler.dto.JobInfoData;
import org.ow2.proactive_grid_cloud_portal.scheduler.dto.JobResultData;
import org.ow2.proactive_grid_cloud_portal.scheduler.dto.JobStateData;
import org.ow2.proactive_grid_cloud_portal.scheduler.dto.JobUsageData;
import org.ow2.proactive_grid_cloud_portal.scheduler.dto.JobValidationData;
import org.ow2.proactive_grid_cloud_portal.scheduler.dto.SchedulerStatusData;
import org.ow2.proactive_grid_cloud_portal.scheduler.dto.SchedulerUserData;
import org.ow2.proactive_grid_cloud_portal.scheduler.dto.TaskResultData;
import org.ow2.proactive_grid_cloud_portal.scheduler.dto.TaskStateData;
import org.ow2.proactive_grid_cloud_portal.scheduler.dto.UserJobData;
import org.ow2.proactive_grid_cloud_portal.scheduler.exception.JobAlreadyFinishedRestException;
import org.ow2.proactive_grid_cloud_portal.scheduler.exception.JobCreationRestException;
import org.ow2.proactive_grid_cloud_portal.scheduler.exception.LogForwardingRestException;
import org.ow2.proactive_grid_cloud_portal.scheduler.exception.NotConnectedRestException;
import org.ow2.proactive_grid_cloud_portal.scheduler.exception.PermissionRestException;
import org.ow2.proactive_grid_cloud_portal.scheduler.exception.SchedulerRestException;
import org.ow2.proactive_grid_cloud_portal.scheduler.exception.SubmissionClosedRestException;
import org.ow2.proactive_grid_cloud_portal.scheduler.exception.UnknownJobRestException;
import org.ow2.proactive_grid_cloud_portal.scheduler.exception.UnknownTaskRestException;
import org.ow2.proactive_grid_cloud_portal.webapp.DateFormatter;
import org.ow2.proactive_grid_cloud_portal.webapp.PortalConfiguration;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.Selectors;
import org.apache.log4j.Logger;
import org.dozer.DozerBeanMapper;
import org.dozer.Mapper;
import org.jboss.resteasy.annotations.GZIP;
import org.jboss.resteasy.annotations.providers.multipart.MultipartForm;
import org.jboss.resteasy.plugins.providers.multipart.InputPart;
import org.jboss.resteasy.plugins.providers.multipart.MultipartFormDataInput;
import org.jboss.resteasy.util.GenericType;

import static javax.ws.rs.core.MediaType.APPLICATION_XML_TYPE;
import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;
import static org.ow2.proactive_grid_cloud_portal.scheduler.ValidationUtil.validateJobDescriptor;


/**
 * this class exposes the Scheduler as a RESTful service.
 */
@Path("/scheduler/")
public class SchedulerStateRest implements SchedulerRestInterface {

    /**
     * If the rest api was unable to instantiate the value from byte array
     * representation
     */
    public static final String UNKNOWN_VALUE_TYPE = "Unknown value type";

    private static final Logger logger = ProActiveLogger.getLogger(SchedulerStateRest.class);

    private SessionStore sessionStore = SharedSessionStore.getInstance();

    private static FileSystemManager fsManager = null;

    static {
        try {
            fsManager = VFSFactory.createDefaultFileSystemManager();
        } catch (FileSystemException e) {
            e.printStackTrace();
            logger.error("Could not create Default FileSystem Manager", e);
        }
    }

    @SuppressWarnings("unchecked")
    private static final List<SortParameter<JobSortParameter>> DEFAULT_JOB_SORT_PARAMS = Arrays.asList(
            new SortParameter<JobSortParameter>(JobSortParameter.STATE, SortOrder.ASC),
            new SortParameter<JobSortParameter>(JobSortParameter.ID, SortOrder.DESC));

    private static final Mapper mapper = new DozerBeanMapper(Collections
            .singletonList("org/ow2/proactive_grid_cloud_portal/scheduler/dozer-mappings.xml"));

    /**
     * Returns the ids of the current jobs under a list of string.
     * 
     * @param sessionId
     *            a valid session id
     * @param index
     *            optional, if a sublist has to be returned the index of the
     *            sublist
     * @param range
     *            optional, if a sublist has to be returned, the range of the
     *            sublist
     * @return a list of jobs' ids under the form of a list of string
     */
    @GET
    @Path("jobs")
    @Produces("application/json")
    public List<String> jobs(@HeaderParam("sessionid")
    String sessionId, @QueryParam("index")
    @DefaultValue("-1")
    int index, @QueryParam("range")
    @DefaultValue("-1")
    int range) throws NotConnectedRestException, PermissionRestException {
        try {
            Scheduler s = checkAccess(sessionId, "/scheduler/jobs");

            List<JobInfo> jobs = s.getJobs(index, range, new JobFilterCriteria(false, true, true, true),
                    DEFAULT_JOB_SORT_PARAMS);
            List<String> ids = new ArrayList<String>(jobs.size());
            for (JobInfo jobInfo : jobs) {
                ids.add(jobInfo.getJobId().value());
            }

            return ids;
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        }
    }

    /**
     * call a method on the scheduler's frontend in order to renew the lease the
     * user has on this frontend. see PORTAL-70
     * 
     * @throws NotConnectedRestException
     */
    private void renewLeaseForClient(Scheduler scheduler) throws NotConnectedRestException {
        try {
            scheduler.renewSession();
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        }
    }

    /**
     * Returns a subset of the scheduler state, including pending, running,
     * finished jobs (in this particular order). each jobs is described using -
     * its id - its owner - the JobInfo class
     * 
     * @param index
     *            optional, if a sublist has to be returned the index of the
     *            sublist
     * @param range
     *            optional, if a sublist has to be returned, the range of the
     *            sublist
     * @param sessionId
     *            a valid session id
     * @return a list of UserJobData
     */
    @GET
    @Path("jobsinfo")
    @Produces( { "application/json", "application/xml" })
    public List<UserJobData> jobsinfo(@HeaderParam("sessionid")
    String sessionId, @QueryParam("index")
    @DefaultValue("-1")
    int index, @QueryParam("range")
    @DefaultValue("-1")
    int range) throws PermissionRestException, NotConnectedRestException {
        try {
            Scheduler s = checkAccess(sessionId, "/scheduler/jobsinfo");

            List<JobInfo> jobInfoList = s.getJobs(index, range,
                    new JobFilterCriteria(false, true, true, true), DEFAULT_JOB_SORT_PARAMS);
            List<UserJobData> userJobInfoList = new ArrayList<UserJobData>(jobInfoList.size());
            for (JobInfo jobInfo : jobInfoList) {
                userJobInfoList.add(new UserJobData(mapper.map(jobInfo, JobInfoData.class)));
            }

            return userJobInfoList;
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        }
    }

    /**
     * Returns a map containing one entry with the revision id as key and the
     * list of UserJobData as value. each jobs is described using - its id - its
     * owner - the JobInfo class
     * 
     * @param sessionId
     *            a valid session id
     * @param index
     *            optional, if a sublist has to be returned the index of the
     *            sublist
     * @param range
     *            optional, if a sublist has to be returned, the range of the
     *            sublist
     * @param myJobs
     *            fetch only the jobs for the user making the request
     * @param pending
     *            fetch pending jobs
     * @param running
     *            fetch running jobs
     * @param finished
     *            fetch finished jobs
     * @return a map containing one entry with the revision id as key and the
     *         list of UserJobData as value.
     */
    @GET
    @GZIP
    @Path("revisionjobsinfo")
    @Produces( { "application/json", "application/xml" })
    public Map<Long, List<UserJobData>> revisionAndjobsinfo(@HeaderParam("sessionid")
    String sessionId, @QueryParam("index")
    @DefaultValue("-1")
    int index, @QueryParam("range")
    @DefaultValue("-1")
    int range, @QueryParam("myjobs")
    @DefaultValue("false")
    boolean myJobs, @QueryParam("pending")
    @DefaultValue("true")
    boolean pending, @QueryParam("running")
    @DefaultValue("true")
    boolean running, @QueryParam("finished")
    @DefaultValue("true")
    boolean finished) throws PermissionRestException, NotConnectedRestException {
        try {
            Scheduler s = checkAccess(sessionId, "revisionjobsinfo?index=" + index + "&range=" + range);
            String user = sessionStore.get(sessionId).getUserName();

            boolean onlyUserJobs = (myJobs && user != null && user.trim().length() > 0);

            List<JobInfo> jobsInfo = s.getJobs(index, range, new JobFilterCriteria(onlyUserJobs, pending,
                running, finished), DEFAULT_JOB_SORT_PARAMS);
            List<UserJobData> jobs = new ArrayList<UserJobData>(jobsInfo.size());
            for (JobInfo jobInfo : jobsInfo) {
                jobs.add(new UserJobData(mapper.map(jobInfo, JobInfoData.class)));
            }

            HashMap<Long, List<UserJobData>> map = new HashMap<Long, List<UserJobData>>(1);
            map.put(SchedulerStateListener.getInstance().getSchedulerStateRevision(), jobs);
            return map;
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        }
    }

    /**
     * Returns the revision number of the scheduler state
     * 
     * @param sessionId
     *            a valid session id.
     * @return the revision of the scheduler state
     */
    @GET
    @Path("state/revision")
    @Produces( { "application/json", "application/xml" })
    public long schedulerStateRevision(@HeaderParam("sessionid")
    String sessionId) throws NotConnectedRestException {
        Scheduler s = checkAccess(sessionId, "/scheduler/revision");
        renewLeaseForClient(s);
        return SchedulerStateListener.getInstance().getSchedulerStateRevision();
    }

    /**
     * Returns a JobState of the job identified by the id <code>jobid</code>
     * 
     * @param sessionId
     *            a valid session id
     * @param jobId
     *            the id of the job to retrieve
     */
    @GET
    @Path("jobs/{jobid}")
    @Produces( { "application/json", "application/xml" })
    public JobStateData listJobs(@HeaderParam("sessionid")
    String sessionId, @PathParam("jobid")
    String jobId) throws NotConnectedRestException, UnknownJobRestException, PermissionRestException {
        try {
            Scheduler s = checkAccess(sessionId, "/scheduler/jobs/" + jobId);

            JobState js = s.getJobState(jobId);
            js = PAFuture.getFutureValue(js);

            return mapper.map(js, JobStateData.class);
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        } catch (UnknownJobException e) {
            throw new UnknownJobRestException(e);
        }
    }

    /**
     * Stream the output of job identified by the id <code>jobid</code> only
     * stream currently available logs, call this method several times to get
     * the complete output.
     * 
     * @param sessionId
     *            a valid session id
     * @param jobId
     *            the id of the job to retrieve
     * @throws IOException
     * @throws LogForwardingRestException
     */
    @GET
    @GZIP
    @Path("jobs/{jobid}/livelog")
    @Produces("application/json")
    @Override
    public String getLiveLogJob(@HeaderParam("sessionid")
    String sessionId, @PathParam("jobid")
    String jobId) throws NotConnectedRestException, UnknownJobRestException, PermissionRestException,
            LogForwardingRestException, IOException {
        try {
            checkAccess(sessionId, "/scheduler/jobs/" + jobId + "/livelog");
            Session ss = sessionStore.get(sessionId);

            JobOutput jo;
            JobOutputAppender jobOutputAppender = ss.getJobOutputAppender(jobId);
            if (jobOutputAppender == null) {
                jo = JobsOutputController.getInstance().createJobOutput(ss, jobId).getJobOutput();
            } else {
                jo = jobOutputAppender.getJobOutput();
            }

            if (jo != null) {
                return jo.fetchNewLogs();
            }

            return "";
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        } catch (UnknownJobException e) {
            throw new UnknownJobRestException(e);
        } catch (LogForwardingException e) {
            throw new LogForwardingRestException(e);
        }
    }

    /**
     * number of available bytes in the stream or -1 if the stream does not
     * exist.
     * 
     * @param sessionId
     *            a valid session id
     * @param jobId
     *            the id of the job to retrieve
     */
    @Override
    @GET
    @Path("jobs/{jobid}/livelog/available")
    @Produces("application/json")
    public int getLiveLogJobAvailable(@HeaderParam("sessionid")
    String sessionId, @PathParam("jobid")
    String jobId) throws NotConnectedRestException {
        checkAccess(sessionId, "/scheduler/jobs/" + jobId + "/livelog/available");
        Session ss = sessionStore.get(sessionId);

        JobOutputAppender joa = ss.getJobOutputAppender(jobId);
        if (joa != null) {
            return joa.getJobOutput().size();
        }

        return -1;
    }

    /**
     * remove the live log object.
     * 
     * @param sessionId
     *            a valid session id
     * @param jobId
     *            the id of the job to retrieve
     * @throws NotConnectedRestException
     */
    @Override
    @DELETE
    @Path("jobs/{jobid}/livelog")
    @Produces("application/json")
    public boolean deleteLiveLogJob(@HeaderParam("sessionid")
    String sessionId, @PathParam("jobid")
    String jobId) throws NotConnectedRestException {
        checkAccess(sessionId, "delete /scheduler/jobs/livelog" + jobId);
        Session ss = sessionStore.get(sessionId);
        ss.removeJobOutAppender(jobId);
        return true;

    }

    /**
     * Returns the job result associated to the job referenced by the id
     * <code>jobid</code>
     * 
     * @param sessionId
     *            a valid session id
     * @return the job result of the corresponding job
     */
    @GET
    @GZIP
    @Path("jobs/{jobid}/result")
    @Produces("application/json")
    public JobResultData jobResult(@HeaderParam("sessionid")
    String sessionId, @PathParam("jobid")
    String jobId) throws NotConnectedRestException, PermissionRestException, UnknownJobRestException {
        try {
            Scheduler s = checkAccess(sessionId, "jobs/" + jobId + "/result");
            return mapper.map(PAFuture.getFutureValue(s.getJobResult(jobId)), JobResultData.class);
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (UnknownJobException e) {
            throw new UnknownJobRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        }
    }

    /**
     * Returns all the task results of this job as a map whose the key is the
     * name of the task and its task result.<br>
     * If the result cannot be instantiated, the content is replaced by the
     * string 'Unknown value type'. To get the serialized form of a given
     * result, one has to call the following restful service
     * jobs/{jobid}/tasks/{taskname}/result/serializedvalue
     * 
     * @param sessionId
     *            a valid session id
     * @param jobId
     *            a job id
     */
    @GET
    @GZIP
    @Path("jobs/{jobid}/result/value")
    @Produces("application/json")
    public Map<String, String> jobResultValue(@HeaderParam("sessionid")
    String sessionId, @PathParam("jobid")
    String jobId) throws NotConnectedRestException, PermissionRestException, UnknownJobRestException {
        try {
            Scheduler s = checkAccess(sessionId, "jobs/" + jobId + "/result/value");
            JobResult jobResult = PAFuture.getFutureValue(s.getJobResult(jobId));
            if (jobResult == null) {
                return null;
            }
            Map<String, TaskResult> allResults = jobResult.getAllResults();
            Map<String, String> res = new HashMap<String, String>(allResults.size());
            for (final Entry<String, TaskResult> entry : allResults.entrySet()) {
                TaskResult taskResult = entry.getValue();
                String value = getTaskResultValueAsStringOrExceptionStackTrace(taskResult);
                res.put(entry.getKey(), value);
            }
            return res;
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (UnknownJobException e) {
            throw new UnknownJobRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        }
    }

    /**
     * Delete a job
     * 
     * @param sessionId
     *            a valid session id
     * @param jobId
     *            the id of the job to delete
     * @return true if success, false if the job not yet finished (not removed,
     *         kill the job then remove it)
     * 
     */
    @DELETE
    @Path("jobs/{jobid}")
    @Produces("application/json")
    public boolean removeJob(@HeaderParam("sessionid")
    String sessionId, @PathParam("jobid")
    String jobId) throws NotConnectedRestException, UnknownJobRestException, PermissionRestException {
        try {
            Scheduler s = checkAccess(sessionId, "DELETE jobs/" + jobId);
            return s.removeJob(jobId);
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (UnknownJobException e) {
            throw new UnknownJobRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        }
    }

    /**
     *  Returns job server logs
     * @param sessionId a valid session id
     * @param jobId the id of the job
     * @return job traces from the scheduler and resource manager
    */
    @GET
    @GZIP
    @Path("jobs/{jobid}/log/server")
    @Produces("application/json")
    public String jobServerLog(@HeaderParam("sessionid")
    String sessionId, @PathParam("jobid")
    String jobId) throws NotConnectedRestException, UnknownJobRestException, PermissionRestException {
        try {
            Scheduler s = checkAccess(sessionId, "jobs/" + jobId + "/log/server");
            return s.getJobServerLogs(jobId);
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (UnknownJobException e) {
            throw new UnknownJobRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        }
    }

    /**
     * Kill the job represented by jobId.<br>
     * 
     * @param sessionId
     *            a valid session id
     * @param jobId
     *            the job to kill.
     * @return true if success, false if not.
     */
    @PUT
    @Path("jobs/{jobid}/kill")
    @Produces("application/json")
    public boolean killJob(@HeaderParam("sessionid")
    String sessionId, @PathParam("jobid")
    String jobId) throws NotConnectedRestException, UnknownJobRestException, PermissionRestException {
        try {
            Scheduler s = checkAccess(sessionId, "PUT jobs/" + jobId + "/kill");
            return s.killJob(jobId);
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (UnknownJobException e) {
            throw new UnknownJobRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        }
    }

    /**
     * Kill a task within a job
     * @param sessionId current session
     * @param jobid id of the job containing the task to kill
     * @param taskname name of the task to kill
     * @throws UnknownJobRestException
     * @throws UnknownTaskRestException
     * @throws PermissionRestException
     * @throws NotConnectedRestException
     */
    @PUT
    @Path("jobs/{jobid}/tasks/{taskname}/kill")
    public boolean killTask(@HeaderParam("sessionid")
    String sessionId, @PathParam("jobid")
    String jobid, @PathParam("taskname")
    String taskname) throws NotConnectedRestException, UnknownJobRestException, UnknownTaskRestException,
            PermissionRestException {
        try {
            Scheduler s = checkAccess(sessionId, "PUT jobs/" + jobid + "/tasks/" + taskname + "/kill");
            return s.killTask(jobid, taskname);
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (UnknownJobException e) {
            throw new UnknownJobRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        } catch (UnknownTaskException e) {
            throw new UnknownTaskRestException(e);
        }
    }

    /**
     * Preempt a task within a job
     * <p>
     * The task will be stopped and restarted later
     * @param sessionId current session
     * @param jobid id of the job containing the task to preempt
     * @param taskname name of the task to preempt
     * @throws NotConnectedRestException
     * @throws org.ow2.proactive_grid_cloud_portal.scheduler.exception.UnknownJobRestException
     * @throws org.ow2.proactive_grid_cloud_portal.scheduler.exception.UnknownTaskRestException
     * @throws org.ow2.proactive_grid_cloud_portal.scheduler.exception.PermissionRestException
     */
    @Override
    @PUT
    @Path("jobs/{jobid}/tasks/{taskname}/preempt")
    public boolean preemptTask(@HeaderParam("sessionid")
    String sessionId, @PathParam("jobid")
    String jobid, @PathParam("taskname")
    String taskname) throws NotConnectedRestException, UnknownJobRestException, UnknownTaskRestException,
            PermissionRestException {
        try {
            Scheduler s = checkAccess(sessionId, "PUT jobs/" + jobid + "/tasks/" + taskname + "/preempt");
            return s.preemptTask(jobid, taskname, 5);
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (UnknownJobException e) {
            throw new UnknownJobRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        } catch (UnknownTaskException e) {
            throw new UnknownTaskRestException(e);
        }
    }

    /**
     * Restart a task within a job
     * @param sessionId current session
     * @param jobid id of the job containing the task to kill
     * @param taskname name of the task to kill
     * @throws NotConnectedRestException
     * @throws UnknownJobRestException
     * @throws UnknownTaskRestException
     * @throws PermissionRestException
     */
    @Override
    @PUT
    @Path("jobs/{jobid}/tasks/{taskname}/restart")
    public boolean restartTask(@HeaderParam("sessionid")
    String sessionId, @PathParam("jobid")
    String jobid, @PathParam("taskname")
    String taskname) throws NotConnectedRestException, UnknownJobRestException, UnknownTaskRestException,
            PermissionRestException {
        try {
            Scheduler s = checkAccess(sessionId, "PUT jobs/" + jobid + "/tasks/" + taskname + "/restart");
            return s.restartTask(jobid, taskname, 5);
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (UnknownJobException e) {
            throw new UnknownJobRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        } catch (UnknownTaskException e) {
            throw new UnknownTaskRestException(e);
        }
    }

    /**
     * Returns a list of the name of the tasks belonging to job
     * <code>jobId</code>
     * 
     * @param sessionId
     *            a valid session id
     * @param jobId
     *            jobid one wants to list the tasks' name
     * @return a list of tasks' name
     */
    @GET
    @Path("jobs/{jobid}/tasks")
    @Produces("application/json")
    public List<String> getJobTasksIds(@HeaderParam("sessionid")
    String sessionId, @PathParam("jobid")
    String jobId) throws NotConnectedRestException, UnknownJobRestException, PermissionRestException {
        try {
            Scheduler s = checkAccess(sessionId, "jobs/" + jobId + "/tasks");

            JobState jobState = s.getJobState(jobId);
            List<String> tasksName = new ArrayList<String>(jobState.getTasks().size());
            for (TaskState ts : jobState.getTasks()) {
                tasksName.add(ts.getId().getReadableName());
            }

            return tasksName;
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (UnknownJobException e) {
            throw new UnknownJobRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @GET
    @Path("jobs/{jobid}/image")
    @Produces("application/json;charset=" + ENCODING)
    public String getJobImage(@HeaderParam("sessionid")
    String sessionId, @PathParam("jobid")
    String jobId) throws IOException, NotConnectedRestException {
        checkAccess(sessionId);

        ZipFile jobArchive = new ZipFile(PortalConfiguration.jobIdToPath(jobId));
        ZipEntry imageEntry = jobArchive.getEntry("JOB-INF/image.png");
        if (imageEntry == null) {
            throw new IOException("the file JOB-INF/image.png was not found in the job archive");
        }
        InputStream ips = new BufferedInputStream(jobArchive.getInputStream(imageEntry));
        // Encode in Base64
        return new String(Base64.encodeBase64(IOUtils.toByteArray(ips)), ENCODING);
    }

    /**
     * The job map is a XML file generated by the Studio describing the
     * visual representation of a workflow.
     * 
     * @throws IOException
     *             when the job archive is not found
     * @param sessionId
     *            a valid session id
     * @param jobId
     *            the job id
     * @return the map corresponding to the <code>jobId</code>
     */
    @GET
    @GZIP
    @Path("jobs/{jobid}/map")
    @Produces( { "application/json", "application/xml" })
    public String getJobMap(@HeaderParam("sessionid")
    String sessionId, @PathParam("jobid")
    String jobId) throws IOException, NotConnectedRestException {
        checkAccess(sessionId);

        String map = "";
        InputStream ips;

        ZipFile jobArchive = new ZipFile(PortalConfiguration.jobIdToPath(jobId));
        ZipEntry imageEntry = jobArchive.getEntry("JOB-INF/map.xml");
        if (imageEntry == null) {
            throw new IOException("the file JOB-INF/map.xml was not found in the job archive");
        }
        ips = new BufferedInputStream(jobArchive.getInputStream(imageEntry));
        InputStreamReader ipsr = new InputStreamReader(ips);
        BufferedReader br = new BufferedReader(ipsr);
        String ligne;
        while ((ligne = br.readLine()) != null) {
            map += ligne + "\n";
        }
        br.close();

        return map;
    }

    /**
     * {@inheritDoc}
     */
    @GET
    @Path("jobs/{jobid}/html")
    @Produces("text/html")
    public String getJobHtml(@HeaderParam("sessionid")
    String sessionId, @PathParam("jobid")
    String jobId) throws IOException, NotConnectedRestException {
        checkAccess(sessionId);

        File jobHtml = new File(PortalConfiguration.jobIdToPath(jobId) + ".html");
        if (!jobHtml.exists()) {
            throw new IOException("the file " + jobHtml.getAbsolutePath() + " was not found on the server");
        }
        InputStream ips = new BufferedInputStream(new FileInputStream(jobHtml));
        return new String(IOUtils.toByteArray(ips));
    }

    /**
     * Returns a list of taskState
     * 
     * @param sessionId
     *            a valid session id
     * @param jobId
     *            the job id
     * @return a list of task' states of the job <code>jobId</code>
     */
    @GET
    @GZIP
    @Path("jobs/{jobid}/taskstates")
    @Produces("application/json")
    public List<TaskStateData> getJobTaskStates(@HeaderParam("sessionid")
    String sessionId, @PathParam("jobid")
    String jobId) throws NotConnectedRestException, UnknownJobRestException, PermissionRestException {
        try {
            Scheduler s = checkAccess(sessionId, "jobs/" + jobId + "/taskstates");
            JobState jobState = s.getJobState(jobId);
            return map(jobState.getTasks(), TaskStateData.class);
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (UnknownJobException e) {
            throw new UnknownJobRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        }
    }

    /**
     *  Returns full logs generated by tasks in job.
     *
     * @param sessionId a valid session id
     * @param jobId the id of the job
     * @return  all the logs generated by the tasks
     */
    @GET
    @GZIP
    @Path("jobs/{jobid}/log/full")
    @Produces("application/json")
    public InputStream jobFullLogs(@HeaderParam("sessionid")
    String sessionId, @PathParam("jobid")
    String jobId, @QueryParam("sessionid")
    String session) throws NotConnectedRestException, UnknownJobRestException, UnknownTaskRestException,
            PermissionRestException, IOException {

        if (sessionId == null) {
            sessionId = session;
        }

        try {
            Scheduler s = checkAccess(sessionId, "jobs/" + jobId + "/log/all");

            JobState jobState = s.getJobState(jobId);
            List<String> tasksName = new ArrayList<String>(jobState.getTasks().size());

            Set<InputStream> streams = new HashSet<InputStream>();
            for (TaskState ts : jobState.getTasks()) {
                String fullTaskLogsFile = "TaskLogs-" + jobId + "-" + ts.getId() + ".log";
                streams.add(pullFile(sessionId, SchedulerConstants.USERSPACE_NAME, fullTaskLogsFile));
            }

            return new SequenceInputStream(Collections.enumeration(streams));
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (UnknownJobException e) {
            throw new UnknownJobRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        }

    }

    /**
     * Return the task state of the task <code>taskname</code> of the job
     * <code>jobId</code>
     * 
     * @param sessionId
     *            a valid session id
     * @param jobId
     *            the id of the job
     * @param taskname
     *            the name of the task
     * @return the task state of the task <code>taskname</code> of the job
     *         <code>jobId</code>
     */
    @GET
    @Path("jobs/{jobid}/tasks/{taskname}")
    @Produces("application/json")
    public TaskStateData jobtasks(@HeaderParam("sessionid")
    String sessionId, @PathParam("jobid")
    String jobId, @PathParam("taskname")
    String taskname) throws NotConnectedRestException, UnknownJobRestException, PermissionRestException,
            UnknownTaskRestException {
        try {
            Scheduler s = checkAccess(sessionId, "jobs/" + jobId + "/tasks/" + taskname);

            JobState jobState = s.getJobState(jobId);

            for (TaskState ts : jobState.getTasks()) {
                if (ts.getId().getReadableName().equals(taskname)) {
                    return mapper.map(ts, TaskStateData.class);
                }
            }

            throw new UnknownTaskRestException("task " + taskname + "not found");
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (UnknownJobException e) {
            throw new UnknownJobRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        }
    }

    /**
     * Returns the value of the task result of task <code>taskName</code> of the
     * job <code>jobId</code> <strong>the result is deserialized before sending
     * to the client, if the class is not found the content is replaced by the
     * string 'Unknown value type' </strong>. To get the serialized form of a
     * given result, one has to call the following restful service
     * jobs/{jobid}/tasks/{taskname}/result/serializedvalue
     * 
     * @param sessionId
     *            a valid session id
     * @param jobId
     *            the id of the job
     * @param taskname
     *            the name of the task
     * @return the value of the task result
     */
    @GET
    @GZIP
    @Path("jobs/{jobid}/tasks/{taskname}/result/value")
    @Produces("*/*")
    public Serializable valueOftaskresult(@HeaderParam("sessionid")
    String sessionId, @PathParam("jobid")
    String jobId, @PathParam("taskname")
    String taskname) throws Throwable {
        Scheduler s = checkAccess(sessionId, "jobs/" + jobId + "/tasks/" + taskname + "/result/value");
        TaskResult taskResult = s.getTaskResult(jobId, taskname);
        return getTaskResultValueAsStringOrExceptionStackTrace(taskResult);
    }

    private String getTaskResultValueAsStringOrExceptionStackTrace(TaskResult taskResult) {
        if (taskResult == null) {
            // task is not finished yet
            return null;
        }
        String value = null;
        // No entry if the task had exception
        if (taskResult.hadException()) {
            value = StackTraceUtil.getStackTrace(taskResult.getException());
        } else {
            try {
                Serializable instanciatedValue = taskResult.value();
                if (instanciatedValue != null) {
                    value = instanciatedValue.toString();
                }
            } catch (InternalSchedulerException e) {
                value = UNKNOWN_VALUE_TYPE;
            } catch (Throwable t) {
                value = "Unable to get the value due to " + t.getMessage();
            }
        }
        return value;
    }

    /**
     * Returns the value of the task result of the task <code>taskName</code> of
     * the job <code>jobId</code> This method returns the result as a byte array
     * whatever the result is.
     * 
     * @param sessionId
     *            a valid session id
     * @param jobId
     *            the id of the job
     * @param taskname
     *            the name of the task
     * @return the value of the task result as a byte array.
     */
    @GET
    @GZIP
    @Path("jobs/{jobid}/tasks/{taskname}/result/serializedvalue")
    @Produces("*/*")
    public byte[] serializedValueOftaskresult(@HeaderParam("sessionid")
    String sessionId, @PathParam("jobid")
    String jobId, @PathParam("taskname")
    String taskname) throws Throwable {
        Scheduler s = checkAccess(sessionId, "jobs/" + jobId + "/tasks/" + taskname +
            "/result/serializedvalue");
        TaskResult tr = s.getTaskResult(jobId, taskname);
        tr = PAFuture.getFutureValue(tr);
        return tr.getSerializedValue();
    }

    /**
     * Returns the task result of the task <code>taskName</code> of the job
     * <code>jobId</code>
     * 
     * @param sessionId
     *            a valid session id
     * @param jobId
     *            the id of the job
     * @param taskname
     *            the name of the task
     * @return the task result of the task <code>taskName</code>
     */
    @GET
    @GZIP
    @Path("jobs/{jobid}/tasks/{taskname}/result")
    @Produces("application/json")
    public TaskResultData taskresult(@HeaderParam("sessionid")
    String sessionId, @PathParam("jobid")
    String jobId, @PathParam("taskname")
    String taskname) throws NotConnectedRestException, UnknownJobRestException, UnknownTaskRestException,
            PermissionRestException {
        try {
            Scheduler s = checkAccess(sessionId, "jobs/" + jobId + "/tasks/" + taskname + "/result");
            TaskResult tr = s.getTaskResult(jobId, taskname);
            return mapper.map(PAFuture.getFutureValue(tr), TaskResultData.class);
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (UnknownJobException e) {
            throw new UnknownJobRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        } catch (UnknownTaskException e) {
            throw new UnknownTaskRestException(e);
        }
    }

    /**
     * Returns all the logs generated by the task (either stdout and stderr)
     * 
     * @param sessionId
     *            a valid session id
     * @param jobId
     *            the id of the job
     * @param taskname
     *            the name of the task
     * @return all the logs generated by the task (either stdout and stderr) or
     *         an empty string if the result is not yet available
     */
    @GET
    @GZIP
    @Path("jobs/{jobid}/tasks/{taskname}/result/log/all")
    @Produces("application/json")
    public String tasklog(@HeaderParam("sessionid")
    String sessionId, @PathParam("jobid")
    String jobId, @PathParam("taskname")
    String taskname) throws NotConnectedRestException, UnknownJobRestException, UnknownTaskRestException,
            PermissionRestException {
        try {
            Scheduler s = checkAccess(sessionId, "jobs/" + jobId + "/tasks/" + taskname + "/result/log/all");
            TaskResult tr = s.getTaskResult(jobId, taskname);
            if ((tr != null) && (tr.getOutput() != null)) {
                return tr.getOutput().getAllLogs(true);
            } else {
                return "";
            }
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (UnknownJobException e) {
            throw new UnknownJobRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        } catch (UnknownTaskException e) {
            throw new UnknownTaskRestException(e);
        }
    }

    /**
     * Returns the standard error output (stderr) generated by the task
     * 
     * @param sessionId
     *            a valid session id
     * @param jobId
     *            the id of the job
     * @param taskname
     *            the name of the task
     * @return the stderr generated by the task or an empty string if the result
     *         is not yet available
     */
    @GET
    @GZIP
    @Path("jobs/{jobid}/tasks/{taskname}/result/log/err")
    @Produces("application/json")
    public String tasklogErr(@HeaderParam("sessionid")
    String sessionId, @PathParam("jobid")
    String jobId, @PathParam("taskname")
    String taskname) throws NotConnectedRestException, UnknownJobRestException, UnknownTaskRestException,
            PermissionRestException {
        try {
            Scheduler s = checkAccess(sessionId, "jobs/" + jobId + "/tasks/" + taskname + "/result/log/err");
            TaskResult tr = s.getTaskResult(jobId, taskname);
            if ((tr != null) && (tr.getOutput() != null)) {
                return tr.getOutput().getStderrLogs(true);
            } else {
                return "";
            }
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (UnknownJobException e) {
            throw new UnknownJobRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        } catch (UnknownTaskException e) {
            throw new UnknownTaskRestException(e);
        }
    }

    /**
     * Returns the standard output (stderr) generated by the task
     * 
     * @param sessionId
     *            a valid session id
     * @param jobId
     *            the id of the job
     * @param taskname
     *            the name of the task
     * @return the stdout generated by the task or an empty string if the result
     *         is not yet available
     */
    @GET
    @GZIP
    @Path("jobs/{jobid}/tasks/{taskname}/result/log/out")
    @Produces("application/json")
    public String tasklogout(@HeaderParam("sessionid")
    String sessionId, @PathParam("jobid")
    String jobId, @PathParam("taskname")
    String taskname) throws NotConnectedRestException, UnknownJobRestException, UnknownTaskRestException,
            PermissionRestException {
        try {
            Scheduler s = checkAccess(sessionId, "jobs/" + jobId + "/tasks/" + taskname + "/result/log/out");
            TaskResult tr = s.getTaskResult(jobId, taskname);
            if ((tr != null) && (tr.getOutput() != null)) {
                return tr.getOutput().getStdoutLogs(true);
            } else {
                return "";
            }
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (UnknownJobException e) {
            throw new UnknownJobRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        } catch (UnknownTaskException e) {
            throw new UnknownTaskRestException(e);
        }
    }

    /**
     * Returns full logs generated by the task from user data spaces.
     *
     *
     * @param sessionId
     *            a valid session id
     * @param jobId
     *            the id of the job
     * @param taskname
     *            the name of the task
     * @return all the logs generated by the task (either stdout and stderr) or
     *         an empty string if the result is not yet available
     */
    @GET
    @GZIP
    @Path("jobs/{jobid}/tasks/{taskname}/result/log/full")
    @Produces("application/json")
    public InputStream taskFullLogs(@HeaderParam("sessionid")
    String sessionId, @PathParam("jobid")
    String jobId, @PathParam("taskname")
    String taskname, @QueryParam("sessionid")
    String session) throws NotConnectedRestException, UnknownJobRestException, UnknownTaskRestException,
            PermissionRestException, IOException {
        try {

            if (sessionId == null) {
                sessionId = session;
            }

            Scheduler s = checkAccess(sessionId, "jobs/" + jobId + "/tasks/" + taskname + "/result/log/all");
            TaskResult tr = s.getTaskResult(jobId, taskname);

            if (tr != null) {
                String fullTaskLogsFile = "TaskLogs-" + jobId + "-" + tr.getTaskId() + ".log";
                return pullFile(sessionId, SchedulerConstants.USERSPACE_NAME, fullTaskLogsFile);
            } else {
                return null;
            }
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (UnknownJobException e) {
            throw new UnknownJobRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        } catch (UnknownTaskException e) {
            throw new UnknownTaskRestException(e);
        }
    }

    /**
     *  Returns task server logs
     * @param sessionId a valid session id
     * @param jobId the id of the job
     * @param taskname the name of the task
     * @return task traces from the scheduler and resource manager
    */
    @GET
    @GZIP
    @Path("jobs/{jobid}/tasks/{taskname}/log/server")
    @Produces("application/json")
    public String taskServerLog(@HeaderParam("sessionid")
    String sessionId, @PathParam("jobid")
    String jobId, @PathParam("taskname")
    String taskname) throws NotConnectedRestException, UnknownJobRestException, UnknownTaskRestException,
            PermissionRestException {
        try {
            Scheduler s = checkAccess(sessionId, "jobs/" + jobId + "/tasks/" + taskname + "/log/server");
            return s.getTaskServerLogs(jobId, taskname);
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (UnknownJobException e) {
            throw new UnknownJobRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        } catch (UnknownTaskException e) {
            throw new UnknownTaskRestException(e);
        }
    }

    /**
     * the method check is the session id is valid i.e. a scheduler client is
     * associated to the session id in the session map. If not, a
     * NotConnectedRestException is thrown specifying the invalid access *
     * 
     * @return the scheduler linked to the session id, an NotConnectedRestException,
     *         if no such mapping exists.
     * @throws NotConnectedRestException
     */
    private SchedulerProxyUserInterface checkAccess(String sessionId, String path)
            throws NotConnectedRestException {
        Session ss = sessionStore.get(sessionId);
        if (ss == null) {
            throw new NotConnectedRestException(
                "you are not connected to the scheduler, you should log on first");
        }

        SchedulerProxyUserInterface s = ss.getScheduler();

        if (s == null) {
            throw new NotConnectedRestException(
                "you are not connected to the scheduler, you should log on first");
        }
        return s;
    }

    private SchedulerProxyUserInterface checkAccess(String sessionId) throws NotConnectedRestException {
        return checkAccess(sessionId, "");
    }

    /**
     * Pauses the job represented by jobid
     * 
     * @param sessionId
     *            a valid session id
     * @param jobId
     *            the id of the job
     * @return true if success, false if not
     */
    @PUT
    @Path("jobs/{jobid}/pause")
    @Produces("application/json")
    public boolean pauseJob(@HeaderParam("sessionid")
    final String sessionId, @PathParam("jobid")
    final String jobId) throws NotConnectedRestException, UnknownJobRestException, PermissionRestException {
        try {
            final Scheduler s = checkAccess(sessionId, "POST jobs/" + jobId + "/pause");
            return s.pauseJob(jobId);
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (UnknownJobException e) {
            throw new UnknownJobRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        }
    }

    /**
     * Resumes the job represented by jobid
     * 
     * @param sessionId
     *            a valid session id
     * @param jobId
     *            the id of the job
     * @return true if success, false if not
     */
    @PUT
    @Path("jobs/{jobid}/resume")
    @Produces("application/json")
    public boolean resumeJob(@HeaderParam("sessionid")
    final String sessionId, @PathParam("jobid")
    final String jobId) throws NotConnectedRestException, UnknownJobRestException, PermissionRestException {
        try {
            Scheduler s = checkAccess(sessionId, "POST jobs/" + jobId + "/resume");
            return s.resumeJob(jobId);
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (UnknownJobException e) {
            throw new UnknownJobRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        }
    }

    /**
     * Submit job using flat command file
     * @param sessionId valid session id
     * @param commandFileContent content of a command file: endline separated native commands
     * @param jobName name of the job to create
     * @param selectionScriptContent content of a selection script, or null
     * @param selectionScriptExtension extension of the selectionscript to determine script engine ("js", "py", "rb")
     * @return Id of the submitted job
     * @throws NotConnectedRestException
     * @throws IOException
     * @throws JobCreationRestException
     * @throws PermissionRestException
     * @throws SubmissionClosedRestException
     */
    @POST
    @Path("submitflat")
    @Produces("application/json")
    public JobIdData submitFlat(@HeaderParam("sessionid")
    String sessionId, @FormParam("commandFileContent")
    String commandFileContent, @FormParam("jobName")
    String jobName, @FormParam("selectionScriptContent")
    String selectionScriptContent, @FormParam("selectionScriptExtension")
    String selectionScriptExtension) throws NotConnectedRestException, IOException, JobCreationRestException,
            PermissionRestException, SubmissionClosedRestException {
        Scheduler s = checkAccess(sessionId, "submitflat");

        try {
            File command = File.createTempFile("flatsubmit_commands_", ".txt");
            command.deleteOnExit();

            String selectionPath = null;
            File selection = null;

            if (selectionScriptContent != null && selectionScriptContent.trim().length() > 0) {
                selection = File.createTempFile("flatsubmit_selection_", "." + selectionScriptExtension);
                selection.deleteOnExit();
                PrintWriter pw = new PrintWriter(new FileOutputStream(selection));
                pw.print(selectionScriptContent);
                pw.close();
                selectionPath = selection.getAbsolutePath();
            }

            PrintWriter pw = new PrintWriter(new FileOutputStream(command));
            pw.print(commandFileContent);
            pw.close();

            Job j = FlatJobFactory.getFactory().createNativeJobFromCommandsFile(command.getAbsolutePath(),
                    jobName, selectionPath, null);
            JobId id = s.submit(j);

            command.delete();
            if (selection != null) {
                selection.delete();
            }

            return mapper.map(id, JobIdData.class);
        } catch (IOException e) {
            throw new IOException("I/O Error: " + e.getMessage(), e);
        } catch (JobCreationException e) {
            throw new JobCreationRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        } catch (SubmissionClosedException e) {
            throw new SubmissionClosedRestException(e);
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        }
    }

    /**
     * Submits a job to the scheduler
     * 
     * @param sessionId
     *            a valid session id
     * @return the <code>jobid</code> of the newly created job
     * @throws IOException
     *             if the job was not correctly uploaded/stored
     */
    @POST
    @Path("submit")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Produces("application/json")
    public JobIdData submit(@HeaderParam("sessionid")
    String sessionId, MultipartFormDataInput multipart) throws JobCreationRestException,
            NotConnectedRestException, PermissionRestException, SubmissionClosedRestException, IOException {
        try {
            Scheduler s = checkAccess(sessionId, "submit");

            Map<String, List<InputPart>> formDataMap = multipart.getFormDataMap();

            String name = formDataMap.keySet().iterator().next();
            File tmp = null;
            try {

                InputPart part1 = multipart.getFormDataMap().get(name).get(0); // "file"
                // is the name of the browser's input field
                InputStream is = part1.getBody(new GenericType<InputStream>() {
                });
                tmp = File.createTempFile("job", "d");

                IOUtils.copy(is, new FileOutputStream(tmp));

                Job j;
                boolean isAnArchive = false;
                if (part1.getMediaType().toString().toLowerCase().contains(
                        MediaType.APPLICATION_XML.toLowerCase())) {
                    // the job sent is the xml file
                    j = JobFactory.getFactory().createJob(tmp.getAbsolutePath());
                } else {
                    // it is a job archive
                    j = JobFactory.getFactory().createJobFromArchive(tmp.getAbsolutePath());
                    isAnArchive = true;
                }
                JobId jobid = s.submit(j);

                File archiveToStore = new File(PortalConfiguration.jobIdToPath(jobid.value()));
                if (isAnArchive) {
                    logger.debug("saving archive to " + archiveToStore.getAbsolutePath());
                    tmp.renameTo(archiveToStore);
                } else {
                    // the job is not an archive, however an archive file can
                    // exist for this new job (due to an old submission)
                    // In that case, we remove the existing file preventing erronous
                    // access
                    // to this previously submitted archive.

                    if (archiveToStore.exists()) {
                        archiveToStore.delete();
                    }
                }

                return mapper.map(jobid, JobIdData.class);

            } finally {
                if (tmp != null) {
                    // clean the temporary file
                    tmp.delete();
                }
            }
        } catch (IOException e) {
            throw new IOException("I/O Error: " + e.getMessage(), e);
        } catch (JobCreationException e) {
            throw new JobCreationRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        } catch (SubmissionClosedException e) {
            throw new SubmissionClosedRestException(e);
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        }
    }

    /**
     * Pushes a file from the local file system into the given DataSpace
     * @param sessionId a valid session id
     * @param spaceName the name of the DataSpace
     * @param filePath the path inside the DataSpace  where to put the file e.g. "/myfolder"
     * @param multipart the form data containing :
     *   - fileName the name of the file that will be created on the DataSpace
     *   - fileContent the content of the file
     * @return true if the transfer succeeded
     * @see org.ow2.proactive.scheduler.common.SchedulerConstants for spaces names
     **/
    @Override
    public boolean pushFile(@HeaderParam("sessionid")
    String sessionId, @PathParam("spaceName")
    String spaceName, @PathParam("filePath")
    String filePath, MultipartFormDataInput multipart) throws IOException, NotConnectedRestException,
            PermissionRestException {
        Scheduler s = checkAccess(sessionId, "pushFile");

        Map<String, List<InputPart>> formDataMap = multipart.getFormDataMap();

        List<InputPart> fNL = formDataMap.get("fileName");
        if ((fNL == null) || (fNL.size() == 0)) {
            throw new IllegalArgumentException("Illegal multipart argument definition (fileName), received " +
                fNL);
        }
        String fileName = fNL.get(0).getBody(String.class, null);

        List<InputPart> fCL = formDataMap.get("fileContent");
        if ((fCL == null) || (fCL.size() == 0)) {
            throw new IllegalArgumentException(
                "Illegal multipart argument definition (fileContent), received " + fCL);
        }
        InputStream fileContent = fCL.get(0).getBody(InputStream.class, null);

        if (fileName == null) {
            throw new IllegalArgumentException("Wrong file name : " + fileName);
        }

        String spaceURI = resolveSpaceUri(s, spaceName);
        if (filePath == null) {
            filePath = "";
        }
        if (!filePath.startsWith("/")) {
            filePath = "/" + filePath;
        }
        String destUri = spaceURI + filePath;
        if (!destUri.endsWith("/")) {
            destUri += "/";
        }
        destUri += fileName;
        FileObject destfo = fsManager.resolveFile(destUri);
        if (!destfo.isWriteable()) {
            RuntimeException ex = new IllegalArgumentException("File " + filePath +
                " is not writable in space " + spaceName);
            logger.error(ex);
            throw ex;
        }
        if (destfo.exists()) {
            destfo.delete();
        }
        // used to create the necessary directories if needed
        destfo.createFile();
        URL targetUrl = destfo.getURL();

        if (targetUrl.toString().startsWith("file:")) {
            // if the url is a file:// url, we push directly the InputStream to the destination file
            File targetFile = null;
            try {
                targetFile = new File(targetUrl.toURI());
            } catch (URISyntaxException e) {
                throw new IllegalStateException(e);
            }
            logger.info("[pushFile] pushing input file to " + targetFile);
            try {
                FileUtils.copyInputStreamToFile(fileContent, targetFile);
            } catch (IOException e) {
                if (targetFile.exists()) {
                    targetFile.delete();
                }
                throw e;
            }
        } else {
            // in the other case, we need to push the inputStream to a tempFile and then transfer the file via dataspaces
            File tmpFile = File.createTempFile("pushedFile", ".tmp");
            try {
                FileUtils.copyInputStreamToFile(fileContent, tmpFile);
                FileObject sourcefo = fsManager.resolveFile(tmpFile.getCanonicalPath());
                destfo.copyFrom(sourcefo, Selectors.SELECT_SELF);
            } finally {
                tmpFile.delete();
            }
        }

        return true;

    }

    /**
     * Either Pulls a file from the given DataSpace to the local file system
     * or list the content of a directory if the path refers to a directory
     * In the case the path to a file is given, the content of this file will be returns as an input stream
     * In the case the path to a directory is given, the input stream returned will be a text stream containing at each line
     * the content of the directory
     * @param sessionId a valid session id
     * @param spaceName the name of the data space involved (GLOBAL or USER)
     * @param filePath the path to the file or directory whose content must be received
     **/
    @Override
    public InputStream pullFile(@HeaderParam("sessionid")
    String sessionId, @PathParam("spaceName")
    String spaceName, @PathParam("filePath")
    String filePath) throws IOException, NotConnectedRestException, PermissionRestException {

        Scheduler s = checkAccess(sessionId, "pullFile");

        String spaceURI = resolveSpaceUri(s, spaceName);
        if (filePath == null) {
            filePath = "";
        }
        if (!filePath.startsWith("/")) {
            filePath = "/" + filePath;
        }
        String destUri = spaceURI + filePath;
        FileObject sourcefo = fsManager.resolveFile(destUri);
        if (!sourcefo.exists() || !sourcefo.isReadable()) {
            RuntimeException ex = new IllegalArgumentException("File " + filePath +
                " does not exist or is not readable in space " + spaceName);
            logger.error(ex);
            throw ex;
        }

        if (sourcefo.getType().equals(FileType.FOLDER)) {
            logger.info("[pullFile] reading directory content from " + sourcefo.getURL());
            // if it's a folder we return an InputStream listing its content
            StringBuilder sb = new StringBuilder();
            String nl = System.getProperty("line.separator");
            for (FileObject fo : sourcefo.getChildren()) {
                sb.append(fo.getName().getBaseName() + nl);

            }
            return IOUtils.toInputStream(sb.toString());

        } else if (sourcefo.getType().equals(FileType.FILE)) {
            logger.info("[pullFile] reading file content from " + sourcefo.getURL());
            return sourcefo.getContent().getInputStream();
        } else {
            RuntimeException ex = new IllegalArgumentException("File " + filePath +
                " has an unsupported type " + sourcefo.getType());
            logger.error(ex);
            throw ex;
        }

    }

    /**
     * Deletes a file or recursively delete a directory from the given DataSpace
     * @param sessionId a valid session id
     * @param spaceName the name of the data space involved (GLOBAL or USER)
     * @param filePath the path to the file or directory which must be deleted
     **/
    @Override
    public boolean deleteFile(@HeaderParam("sessionid")
    String sessionId, @PathParam("spaceName")
    String spaceName, @PathParam("filePath")
    String filePath) throws IOException, NotConnectedRestException, PermissionRestException {
        Scheduler s = checkAccess(sessionId, "deleteFile");

        String spaceURI = resolveSpaceUri(s, spaceName);
        if (filePath == null) {
            filePath = "";
        }
        if (!filePath.startsWith("/")) {
            filePath = "/" + filePath;
        }
        String destUri = spaceURI + filePath;

        FileObject sourcefo = fsManager.resolveFile(destUri);
        if (!sourcefo.exists() || !sourcefo.isWriteable()) {
            RuntimeException ex = new IllegalArgumentException("File or Folder " + filePath +
                " does not exist or is not writable in space " + spaceName);
            logger.error(ex);
            throw ex;
        }
        if (sourcefo.getType().equals(FileType.FILE)) {
            logger.info("[deleteFile] deleting file " + sourcefo.getURL());
            sourcefo.delete();
        } else if (sourcefo.getType().equals(FileType.FOLDER)) {
            logger.info("[deleteFile] deleting folder (and all its descendants) " + sourcefo.getURL());
            sourcefo.delete(Selectors.SELECT_ALL);
        } else {
            RuntimeException ex = new IllegalArgumentException("File " + filePath +
                " has an unsupported type " + sourcefo.getType());
            logger.error(ex);
            throw ex;
        }
        return true;
    }

    private String resolveSpaceUri(Scheduler s, String spaceName) throws NotConnectedRestException,
            PermissionRestException {
        try {
            if (SchedulerConstants.GLOBALSPACE_NAME.equals(spaceName)) {
                return s.getGlobalSpaceURIs().get(0);

            } else if (SchedulerConstants.USERSPACE_NAME.equals(spaceName)) {
                return s.getUserSpaceURIs().get(0);
            } else {
                RuntimeException ex = new IllegalArgumentException("Wrong Data Space name : " + spaceName);
                logger.error(ex);
                throw ex;
            }
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        }
    }

    /**
     * terminates the session id <code>sessionId</code>
     * 
     * @param sessionId
     *            a valid session id
     * @throws NotConnectedRestException
     *             if the scheduler cannot be contacted
     * @throws PermissionRestException
     *             if you are not authorized to perform the action
     */
    @PUT
    @Path("disconnect")
    @Produces("application/json")
    public void disconnect(@HeaderParam("sessionid")
    final String sessionId) throws NotConnectedRestException, PermissionRestException {
        try {
            final Scheduler s = checkAccess(sessionId, "disconnect");
            logger.info("disconnection user " + sessionStore.get(sessionId) + " to session " + sessionId);
            s.disconnect();
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        } finally {
            sessionStore.terminate(sessionId);
            logger.debug("sessionid " + sessionId + " terminated");
        }
    }

    /**
     * pauses the scheduler
     * 
     * @param sessionId
     *            a valid session id
     * @return true if success, false otherwise
     * @throws NotConnectedRestException
     * @throws PermissionRestException
     */
    @PUT
    @Path("pause")
    @Produces("application/json")
    public boolean pauseScheduler(@HeaderParam("sessionid")
    final String sessionId) throws NotConnectedRestException, PermissionRestException {
        try {
            Scheduler s = checkAccess(sessionId, "pause");
            return s.pause();
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        }
    }

    /**
     * stops the scheduler
     * 
     * @param sessionId
     *            a valid session id
     * @return true if success, false otherwise
     * @throws NotConnectedRestException
     * @throws PermissionRestException
     */
    @PUT
    @Path("stop")
    @Produces("application/json")
    public boolean stopScheduler(@HeaderParam("sessionid")
    final String sessionId) throws NotConnectedRestException, PermissionRestException {
        try {
            Scheduler s = checkAccess(sessionId, "stop");
            return s.stop();
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        }
    }

    /**
     * resumes the scheduler
     * 
     * @param sessionId
     *            a valid session id
     * @return true if success, false otherwise
     * @throws NotConnectedRestException
     * @throws PermissionRestException
     */
    @PUT
    @Path("resume")
    @Produces("application/json")
    public boolean resumeScheduler(@HeaderParam("sessionid")
    final String sessionId) throws NotConnectedRestException, PermissionRestException {
        try {
            Scheduler s = checkAccess(sessionId, "resume");
            return s.resume();
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        }
    }

    /**
     * changes the priority of a job
     * 
     * @param sessionId
     *            a valid session id
     * @param jobId
     *            the job id
     * @param priorityName
     *            a string representing the name of the priority
     * @throws NotConnectedRestException
     * @throws UnknownJobRestException
     * @throws PermissionRestException
     * @throws JobAlreadyFinishedRestException
     */
    @PUT
    @Path("jobs/{jobid}/priority/byname/{name}")
    public void schedulerChangeJobPriorityByName(@HeaderParam("sessionid")
    final String sessionId, @PathParam("jobid")
    final String jobId, @PathParam("name")
    String priorityName) throws NotConnectedRestException, UnknownJobRestException, PermissionRestException,
            JobAlreadyFinishedRestException {
        try {
            Scheduler s = checkAccess(sessionId, "jobs/" + jobId + "/priority/byname/" + priorityName);
            s.changeJobPriority(jobId, JobPriority.findPriority(priorityName));
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        } catch (JobAlreadyFinishedException e) {
            throw new JobAlreadyFinishedRestException(e);
        } catch (UnknownJobException e) {
            throw new UnknownJobRestException(e);
        }
    }

    /**
     * changes the priority of a job
     * 
     * @param sessionId
     *            a valid session id
     * @param jobId
     *            the job id
     * @param priorityValue
     *            a string representing the value of the priority
     * @throws NumberFormatException
     * @throws NotConnectedRestException
     * @throws UnknownJobRestException
     * @throws PermissionRestException
     * @throws JobAlreadyFinishedRestException
     */
    @PUT
    @Path("jobs/{jobid}/priority/byvalue/{value}")
    public void schedulerChangeJobPriorityByValue(@HeaderParam("sessionid")
    final String sessionId, @PathParam("jobid")
    final String jobId, @PathParam("value")
    String priorityValue) throws NumberFormatException, NotConnectedRestException, UnknownJobRestException,
            PermissionRestException, JobAlreadyFinishedRestException {
        try {
            Scheduler s = checkAccess(sessionId, "jobs/" + jobId + "/priority/byvalue" + priorityValue);
            s.changeJobPriority(jobId, JobPriority.findPriority(Integer.parseInt(priorityValue)));
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        } catch (JobAlreadyFinishedException e) {
            throw new JobAlreadyFinishedRestException(e);
        } catch (UnknownJobException e) {
            throw new UnknownJobRestException(e);
        }
    }

    /**
     * freezes the scheduler
     * 
     * @param sessionId
     *            a valid session id
     * @return true if success, false otherwise
     * @throws NotConnectedRestException
     * @throws PermissionRestException
     */
    @PUT
    @Path("freeze")
    @Produces("application/json")
    public boolean freezeScheduler(@HeaderParam("sessionid")
    final String sessionId) throws NotConnectedRestException, PermissionRestException {
        try {
            Scheduler s = checkAccess(sessionId, "freeze");
            return s.freeze();
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        }
    }

    /**
     * returns the status of the scheduler
     * 
     * @param sessionId
     *            a valid session id
     * @return the scheduler status
     * @throws NotConnectedRestException
     * @throws PermissionRestException
     */
    @GET
    @Path("status")
    @Produces("application/json")
    public SchedulerStatusData getSchedulerStatus(@HeaderParam("sessionid")
    final String sessionId) throws NotConnectedRestException, PermissionRestException {
        try {
            Scheduler s = checkAccess(sessionId, "status");
            renewLeaseForClient(s);
            return SchedulerStatusData.valueOf(SchedulerStateListener.getInstance().getSchedulerStatus(s)
                    .name());
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        }
    }

    /**
     * starts the scheduler
     * 
     * @param sessionId
     *            a valid session id
     * @return true if success, false otherwise
     * @throws NotConnectedRestException
     * @throws PermissionRestException
     */
    @PUT
    @Path("start")
    @Produces("application/json")
    public boolean startScheduler(@HeaderParam("sessionid")
    final String sessionId) throws NotConnectedRestException, PermissionRestException {
        try {
            Scheduler s = checkAccess(sessionId, "start");
            return s.start();
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        }
    }

    /**
     * kills and shutdowns the scheduler
     * 
     * @param sessionId
     *            a valid session id
     * @return true if success, false if not
     * @throws NotConnectedRestException
     * @throws PermissionRestException
     */
    @PUT
    @Path("kill")
    @Produces("application/json")
    public boolean killScheduler(@HeaderParam("sessionid")
    final String sessionId) throws NotConnectedRestException, PermissionRestException {
        try {
            Scheduler s = checkAccess(sessionId, "kill");
            return s.kill();
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        }
    }

    /**
     * Reconnect a new Resource Manager to the scheduler. Can be used if the
     * resource manager has crashed.
     * 
     * @param sessionId
     *            a valid session id
     * @param rmURL
     *            the url of the resource manager
     * @return true if success, false otherwise.
     * @throws NotConnectedRestException
     * @throws PermissionRestException
     */
    @POST
    @Path("linkrm")
    @Produces("application/json")
    public boolean linkRm(@HeaderParam("sessionid")
    final String sessionId, @FormParam("rmurl")
    String rmURL) throws NotConnectedRestException, PermissionRestException {
        try {
            Scheduler s = checkAccess(sessionId, "linkrm");
            return s.linkResourceManager(rmURL);
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        }
    }

    /**
     * Tests whether or not the user is connected to the ProActive Scheduler
     * 
     * @param sessionId
     *            the session to test
     * @return true if the user connected to a Scheduler, false otherwise.
     * @throws NotConnectedRestException
     */
    @GET
    @Path("isconnected")
    @Produces("application/json")
    public boolean isConnected(@HeaderParam("sessionid")
    final String sessionId) throws NotConnectedRestException {
        Scheduler s = checkAccess(sessionId, "isconnected");
        return s.isConnected();
    }

    /**
     * login to the scheduler using an form containing 2 fields (username &
     * password)
     * 
     * @param username
     *            username
     * @param password
     *            password
     * @return the session id associated to the login
     * @throws LoginException
     */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Path("login")
    @Produces("application/json")
    public String login(@FormParam("username")
    String username, @FormParam("password")
    String password) throws LoginException, SchedulerRestException {
        try {
            if ((username == null) || (password == null)) {
                throw new LoginException("empty login/password");
            }
            Session session = sessionStore.create();
            session.setUserName(username);
            session.connectToScheduler(new CredData(username, password));
            logger.info("binding user " + username + " to session " + session.getSessionId());
            return session.getSessionId();
        } catch (ActiveObjectCreationException e) {
            throw new SchedulerRestException(e);
        } catch (SchedulerException e) {
            throw new SchedulerRestException(e);
        } catch (NodeException e) {
            throw new SchedulerRestException(e);
        }
    }

    /**
     * login to the scheduler using a multipart form can be used either by
     * submitting - 2 fields username & password - a credential file with field
     * name 'credential'
     * 
     * @return the session id associated to this new connection
     * @throws KeyException
     * @throws LoginException
     * @throws SchedulerRestException
     */
    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Path("login")
    @Produces("application/json")
    public String loginWithCredential(@MultipartForm
    LoginForm multipart) throws LoginException, KeyException, SchedulerRestException {
        try {
            Session session = sessionStore.create();

            if (multipart.getCredential() != null) {
                Credentials credentials;
                try {
                    credentials = Credentials.getCredentials(multipart.getCredential());
                    session.connectToScheduler(credentials);
                } catch (IOException e) {
                    throw new LoginException(e.getMessage());
                }
            } else {
                if ((multipart.getUsername() == null) || (multipart.getPassword() == null)) {
                    throw new LoginException("empty login/password");
                }

                session.setUserName(multipart.getUsername());
                CredData credData = new CredData(CredData.parseLogin(multipart.getUsername()), CredData
                        .parseDomain(multipart.getUsername()), multipart.getPassword(), multipart.getSshKey());
                session.connectToScheduler(credData);
            }

            return session.getSessionId();

        } catch (PermissionException e) {
            throw new SchedulerRestException(e);
        } catch (ActiveObjectCreationException e) {
            throw new SchedulerRestException(e);
        } catch (SchedulerException e) {
            throw new SchedulerRestException(e);
        } catch (NodeException e) {
            throw new SchedulerRestException(e);
        }
    }

    /**
     * returns statistics about the scheduler
     * 
     * @param sessionId
     *            the session id associated to this new connection
     * @return a string containing the statistics
     * @throws NotConnectedRestException
     * @throws PermissionRestException
     */
    @GET
    @Path("stats")
    @Produces("application/json")
    public Map<String, String> getStatistics(@HeaderParam("sessionid")
    final String sessionId) throws NotConnectedRestException, PermissionRestException {
        SchedulerProxyUserInterface s = checkAccess(sessionId, "stats");
        return s.getMappedInfo("ProActiveScheduler:name=RuntimeData");
    }

    /**
     * returns a string containing some data regarding the user's account
     * 
     * @param sessionId
     *            the session id associated to this new connection
     * @return a string containing some data regarding the user's account
     * @throws NotConnectedRestException
     * @throws PermissionRestException
     */
    @GET
    @Path("stats/myaccount")
    @Produces("application/json")
    public Map<String, String> getStatisticsOnMyAccount(@HeaderParam("sessionid")
    final String sessionId) throws NotConnectedRestException, PermissionRestException {
        SchedulerProxyUserInterface s = checkAccess(sessionId, "stats/myaccount");
        return s.getMappedInfo("ProActiveScheduler:name=MyAccount");
    }

    /**
     * Users currently connected to the scheduler
     * 
     * @param sessionId
     *            the session id associated to this new connection\
     * @return list of users
     * @throws NotConnectedRestException
     * @throws PermissionRestException
     */
    @GET
    @GZIP
    @Path("users")
    @Produces("application/json")
    public List<SchedulerUserData> getUsers(@HeaderParam("sessionid")
    final String sessionId) throws NotConnectedRestException, PermissionRestException {
        try {
            Scheduler s = checkAccess(sessionId, "users");
            return map(s.getUsers(), SchedulerUserData.class);
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        }
    }

    /**
     * Users having jobs in the scheduler
     * 
     * @param sessionId
     *            the session id associated to this new connection\
     * @return list of users
     * @throws NotConnectedRestException
     * @throws PermissionRestException
     */
    @GET
    @GZIP
    @Path("userswithjobs")
    @Produces("application/json")
    public List<SchedulerUserData> getUsersWithJobs(@HeaderParam("sessionid")
    final String sessionId) throws NotConnectedRestException, PermissionRestException {
        try {
            Scheduler s = checkAccess(sessionId, "userswithjobs");
            return map(s.getUsersWithJobs(), SchedulerUserData.class);
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        }
    }

    private static <T> List<T> map(List<?> toMaps, Class<T> type) {
        List<T> result = new ArrayList<T>();
        for (Object toMap : toMaps) {
            result.add(mapper.map(toMap, type));
        }
        return result;
    }

    /**
     * returns the version of the rest api
     * 
     * @return returns the version of the rest api
     */
    @GET
    @Path("version")
    public String getVersion() {
        return "{ " + "\"scheduler\" : \"" + SchedulerStateRest.class.getPackage().getSpecificationVersion() +
            "\", " + "\"rest\" : \"" + SchedulerStateRest.class.getPackage().getImplementationVersion() +
            "\"" + "}";
    }

    /**
     * generates a credential file from user provided credentials
     * 
     * @return the credential file generated by the scheduler
     * @throws LoginException
     * @throws SchedulerRestException
     */
    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @Path("createcredential")
    @Produces("*/*")
    public byte[] getCreateCredential(@MultipartForm
    LoginForm multipart) throws LoginException, SchedulerRestException {
        try {
            String url = PortalConfiguration.getProperties().getProperty(PortalConfiguration.scheduler_url);

            SchedulerAuthenticationInterface auth = SchedulerConnection.join(url);
            PublicKey pubKey = auth.getPublicKey();

            try {
                Credentials cred = Credentials.createCredentials(new CredData(CredData.parseLogin(multipart
                        .getUsername()), CredData.parseDomain(multipart.getUsername()), multipart
                        .getPassword(), multipart.getSshKey()), pubKey);

                return cred.getBase64();
            } catch (KeyException e) {
                throw new SchedulerRestException(e);
            }
        } catch (ConnectionException e) {
            throw new SchedulerRestException(e);
        }
    }

    @GET
    @Path("usage/myaccount")
    @Produces("application/json")
    @Override
    public List<JobUsageData> getUsageOnMyAccount(@HeaderParam("sessionid")
    String sessionId, @QueryParam("startdate")
    @DateFormatter.DateFormat()
    Date startDate, @QueryParam("enddate")
    @DateFormatter.DateFormat()
    Date endDate) throws NotConnectedRestException, PermissionRestException {
        try {
            Scheduler scheduler = checkAccess(sessionId);
            return map(scheduler.getMyAccountUsage(startDate, endDate), JobUsageData.class);
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        }
    }

    @GET
    @Path("usage/account")
    @Produces("application/json")
    @Override
    public List<JobUsageData> getUsageOnAccount(@HeaderParam("sessionid")
    String sessionId, @QueryParam("user")
    String user, @QueryParam("startdate")
    @DateFormatter.DateFormat()
    Date startDate, @QueryParam("enddate")
    @DateFormatter.DateFormat()
    Date endDate) throws NotConnectedRestException, PermissionRestException {
        try {
            Scheduler scheduler = checkAccess(sessionId);
            return map(scheduler.getAccountUsage(user, startDate, endDate), JobUsageData.class);
        } catch (PermissionException e) {
            throw new PermissionRestException(e);
        } catch (NotConnectedException e) {
            throw new NotConnectedRestException(e);
        }
    }

    @Override
    public JobValidationData validate(MultipartFormDataInput multipart) {
        File tmpFile = null;
        try {
            Map<String, List<InputPart>> formDataMap = multipart.getFormDataMap();
            String name = formDataMap.keySet().iterator().next();
            InputPart part1 = formDataMap.get(name).get(0);
            InputStream is = part1.getBody(new GenericType<InputStream>() {
            });

            tmpFile = File.createTempFile("valid-job", "d");
            IOUtils.copy(is, new FileOutputStream(tmpFile));

            return (APPLICATION_XML_TYPE.equals(part1.getMediaType())) ? validateJobDescriptor(tmpFile)
                    : ValidationUtil.validateJobArchive(tmpFile);
        } catch (IOException e) {
            JobValidationData validation = new JobValidationData();
            validation.setErrorMessage("Cannot read from the job validation request.");
            validation.setStackTrace(getStackTrace(e));
            return validation;
        } finally {
            if (tmpFile != null) {
                tmpFile.delete();
            }
        }
    }

}