package com.redhat.cajun.navy.process;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.arjuna.ats.arjuna.coordinator.TxControl;
import com.redhat.cajun.navy.rules.model.Destinations;
import com.redhat.cajun.navy.rules.model.Incident;
import com.redhat.cajun.navy.rules.model.Mission;
import com.redhat.cajun.navy.rules.model.Responders;
import com.redhat.cajun.navy.rules.model.Status;
import org.junit.BeforeClass;
import org.junit.Test;
import org.kie.api.runtime.manager.RuntimeManager;
import org.kie.api.runtime.process.WorkItem;
import org.kie.api.runtime.process.WorkItemHandler;
import org.kie.api.runtime.process.WorkItemManager;
import org.kie.internal.process.CorrelationKey;

public class IncidentProcessTest extends JbpmBaseTestCase {

    private RuntimeManager mgr;

    private Map<String, WorkItemHandler> workItemHandlers;

    private List<Map<String, Object>> businessRuleTaskParameters = new ArrayList<>();

    private List<Map<String, Object>> sendMessageWihParameters = new ArrayList<>();

    private String incidentId;

    private String responderId;

    private Destinations destinations;

    private Responders responders;

    private int nrAssignments = 0;

    public IncidentProcessTest() {
        super(true, true);
    }

    @BeforeClass
    public static void setupTest() throws Exception {
        TxControl.setXANodeName("node1");
        TxControl.setDefaultTimeout(300);
    }

    /**
     *  Test description:
     *    Given:
     *    When :
     *      an instance of the incident process is started
     *      a mission can be assigned to the incident
     *      the responder assigned to the mission is available
     *    Then:
     *      The ResponderService wih is invoked to get the List of available Responders
     *      The BusinessRuleTask wih is invoked to assign a mission
     *      The SendMessage wih is invoked which sends a VerifyResponderMessage message to Kafka
     *      The process is waiting on a signal with reference ResponderAvailableEvent
     */
    @Test
    public void testIncidentProcess() {

        setup(true);

        incidentId = "incidentId1";
        responderId = "responderId";
        responders = responders();
        destinations = destinations();
        Incident incident = incident(incidentId);

        long pId = startProcess(incident, destinations, "PT60S");

        assertProcessInstanceActive(pId);
        assertNodeTriggered(pId, "Get Active Responders", "Assign Mission", "Set Responder Unavailable");
        assertNodeActive(pId, "signal1");

        verify(workItemHandlers.get("ResponderService")).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));
        verify(workItemHandlers.get("BusinessRuleTask")).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));
        verify(workItemHandlers.get("SendMessage")).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));


        // BusinessRuleTask
        Map<String, Object> params = businessRuleTaskParameters.get(0);
        assertThat(params, notNullValue());
        assertThat(params.get("Language"), equalTo("DRL"));
        assertThat(params.get("KieSessionType"), equalTo("stateless"));
        assertThat(params.get("KieSessionName"), equalTo("cajun-navy-ksession"));
        assertThat(params.get("Incident"), equalTo(incident));
        assertThat(params.get("Destinations"), equalTo(destinations));
        assertThat(params.get("Responders"), equalTo(responders));
        assertThat(params.get("Mission"), notNullValue());
        assertThat(params.get("Mission"), is(instanceOf(Mission.class)));
        Mission mission = (Mission)params.get("Mission");
        assertThat(mission.getStatus(), equalTo(Status.REQUESTED));

        // SendMessageTask
        params = sendMessageWihParameters.get(0);
        assertThat(params, notNullValue());
        assertThat(params.get("MessageType"), equalTo("SetResponderUnavailableCommand"));
        assertThat(params.get("Payload"), notNullValue());
        assertThat(params.get("Payload"), is(instanceOf(Mission.class)));
        Mission m = (Mission) params.get("Payload");
        assertThat(m.getStatus(), equalTo(Status.ASSIGNED));
        assertThat(m.getIncidentId(), equalTo(incidentId));
        assertThat(m.getResponderId(), equalTo(responderId));

    }

    /**
     *  Test description:
     *    Given:
     *    When :
     *      an instance of the incident process is started
     *      a mission can be assigned to the incident
     *      the responder assigned to the mission is available
     *        (the process is signaled with a signal with reference ResponderAvailableEvent and payload true)
     *    Then:
     *      The SendMessage wih is invoked which sends a CreateMissionCommand message to Kafka
     *      The process is ended.
     */
    @Test
    public void testIncidentProcessWhenSignalResponderAvailableEvent() {

        setup(true);

        incidentId = "incidentId2";
        responderId = "responderId";
        responders = responders();
        destinations = destinations();
        Incident incident = incident(incidentId);

        long pId = startProcess(incident, destinations, "PT60S");

        // Signal process
        signalProcess(mgr, "ResponderAvailableEvent", Boolean.TRUE, pId);

        assertProcessInstanceCompleted(pId);

        assertNodeTriggered(pId, "Create Mission Command");

        verify(workItemHandlers.get("SendMessage"), times(2)).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));

        // SendMessageTask
        Map<String, Object> params = sendMessageWihParameters.get(1);
        assertThat(params, notNullValue());
        assertThat(params.get("MessageType"), equalTo("CreateMissionCommand"));
        assertThat(params.get("Payload"), notNullValue());
        assertThat(params.get("Payload"), is(instanceOf(Mission.class)));
        Mission m = (Mission) params.get("Payload");
        assertThat(m.getStatus(), equalTo(Status.ASSIGNED));
        assertThat(m.getIncidentId(), equalTo(incidentId));
        assertThat(m.getResponderId(), equalTo(responderId));
    }

    /**
     *  Test description:
     *    Given:
     *    When :
     *      an instance of the incident process is started
     *      a mission cannot be assigned to the incident
     *    Then:
     *      The ResponderService wih is invoked to get the List of available Responders
     *      The BusinessRuleTask wih is invoked to assign a mission
     *      The process waits in a timer node
     */
    @Test
    public void testIncidentProcessWhenNoAssignment() {
        setup(false);

        incidentId = "incidentId3";
        responderId = "responderId";
        responders = responders();
        destinations = destinations();
        Incident incident = incident(incidentId);

        long pId = startProcess(incident, destinations, "PT60M");

        assertProcessInstanceActive(pId);
        assertNodeTriggered(pId, "Get Active Responders", "Assign Mission");
        assertNodeNotTriggered(pId, "Verify Responder Available command");
        assertNodeActive(pId, "timer");

        verify(workItemHandlers.get("ResponderService")).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));
        verify(workItemHandlers.get("BusinessRuleTask")).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));

        // BusinessRuleTask
        Map<String, Object> params = businessRuleTaskParameters.get(0);
        assertThat(params, notNullValue());
        assertThat(params.get("Language"), equalTo("DRL"));
        assertThat(params.get("KieSessionType"), equalTo("stateless"));
        assertThat(params.get("KieSessionName"), equalTo("cajun-navy-ksession"));
        assertThat(params.get("Incident"), equalTo(incident));
        assertThat(params.get("Destinations"), equalTo(destinations));
        assertThat(params.get("Responders"), equalTo(responders));
        assertThat(params.get("Mission"), notNullValue());
        assertThat(params.get("Mission"), is(instanceOf(Mission.class)));
        Mission mission = (Mission)params.get("Mission");
        assertThat(mission.getStatus(), equalTo(Status.REQUESTED));
    }

    /**
     *  Test description:
     *    Given:
     *    When :
     *      an instance of the incident process is started
     *      a mission can be assigned to the incident
     *      the responder assigned to the mission is not available
     *        (the process is signaled with a signal with reference ResponderAvailableEvent and payload false)
     *    Then:
     *      The ResponderService wih is invoked to get the List of available Responders
     *      The BusinessRuleTask wih is invoked to assign a mission
     *      The SendMessage wih is invoked which sends a VerifyResponderMessage message to Kafka
     *      The process waits in a timer node
     */
    @Test
    public void testIncidentProcessWhenResponderNotAvailable() {
        setup(true);

        incidentId = "incidentId4";
        responderId = "responderId";
        responders = responders();
        destinations = destinations();
        Incident incident = incident(incidentId);

        long pId = startProcess(incident, destinations, "PT60M");

        // Signal process
        signalProcess(mgr, "ResponderAvailableEvent", Boolean.FALSE, pId);

        assertProcessInstanceActive(pId);
        assertNodeNotTriggered(pId, "Create Mission Command");
        assertNodeActive(pId, "timer");
    }

    /**
     *  Test description:
     *    Given:
     *    When :
     *      an instance of the incident process is started
     *      a mission cannot be assigned to the incident
     *      when the assignment is invoked a second time, a mission can be assigned
     *      the responder assigned to the mission is available
     *    Then:
     *      The ResponderService wih is invoked twice to get the List of available Responders
     *      The BusinessRuleTask wih is invoked twice to assign a mission
     *      The SendMessage wih is invoked which sends a VerifyResponderMessage message to Kafka
     *      The process is waiting on a signal with reference ResponderAvailableEvent
     */
    @Test
    public void testIncidentProcessSecondAssignment() throws Exception {

        setup(false);

        incidentId = "incidentId5";
        responderId = "responderId";
        responders = responders();
        destinations = destinations();
        Incident incident = incident(incidentId);

        long pId = startProcess(incident, destinations, "PT1S");

        //wait for timer to fire
        Thread.sleep(5000);

        assertProcessInstanceActive(pId);
        assertNodeTriggered(pId, "Get Active Responders", "Assign Mission", "Set Responder Unavailable", "timer");
        assertNodeActive(pId, "signal1");

        verify(workItemHandlers.get("ResponderService"), times(2)).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));
        verify(workItemHandlers.get("BusinessRuleTask"), times(2)).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));
        verify(workItemHandlers.get("SendMessage")).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));


    }

    private void setup(boolean assigned) {

        if (mgr == null) {
            workItemHandlers = new HashMap<>();

            WorkItemHandler mockResponderServiceWih = mock(WorkItemHandler.class);
            workItemHandlers.put("ResponderService", mockResponderServiceWih);
            doAnswer(invocation -> {
                WorkItem workItem = (WorkItem) invocation.getArguments()[0];
                WorkItemManager workItemManager = (WorkItemManager) invocation.getArguments()[1];
                Map<String, Object> results = new HashMap<>();
                results.put("Responders", responders);
                workItemManager.completeWorkItem(workItem.getId(), results);
                return null;
            }).when(mockResponderServiceWih).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));

            WorkItemHandler mockBusinessRuleTaskWih = mock(WorkItemHandler.class);
            workItemHandlers.put("BusinessRuleTask", mockBusinessRuleTaskWih);
            doAnswer(invocation -> {
                WorkItem workItem = (WorkItem) invocation.getArguments()[0];
                WorkItemManager workItemManager = (WorkItemManager) invocation.getArguments()[1];
                businessRuleTaskParameters.add(workItem.getParameters());
                Map<String, Object> results = new HashMap<>();
                if (nrAssignments > 0) {
                    results.put("Mission", businessRuleTaskResult(true));
                } else {
                    results.put("Mission", businessRuleTaskResult(assigned));
                }
                nrAssignments++;
                workItemManager.completeWorkItem(workItem.getId(), results);
                return null;
            }).when(mockBusinessRuleTaskWih).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));

            WorkItemHandler mockSendMessageWih = mock(WorkItemHandler.class);
            workItemHandlers.put("SendMessage", mockSendMessageWih);
            doAnswer(invocation -> {
                WorkItem workItem = (WorkItem) invocation.getArguments()[0];
                WorkItemManager workItemManager = (WorkItemManager) invocation.getArguments()[1];
                sendMessageWihParameters.add(workItem.getParameters());
                workItemManager.completeWorkItem(workItem.getId(), Collections.emptyMap());
                return null;
            }).when(mockSendMessageWih).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));

            mgr = createRuntimeManager(Strategy.PROCESS_INSTANCE, "test", workItemHandlers, "com/redhat/cajun/navy/process/incident-process.bpmn");
        }
    }

    private Mission businessRuleTaskResult(boolean assigned) {
        Mission mission = new Mission();
        if (assigned) {
            mission.setStatus(Status.ASSIGNED);
            mission.setIncidentId(incidentId);
            mission.setResponderId(responderId);
        } else {
            mission.setStatus(Status.UNASSIGNED);
        }
        return mission;
    }

    private long startProcess(Incident incident, Destinations destinations, String assignmentDelay) {
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("incident", incident);
        parameters.put("destinations", destinations);
        parameters.put("assignmentDelay", assignmentDelay);
        CorrelationKey correlationKey = correlationKeyFactory.newCorrelationKey(incident.getId());
        return startProcess(mgr, "incident-process", correlationKey, parameters);
    }

    private Incident incident(String incidentId) {
        Incident incident = new Incident();
        incident.setId(incidentId);
        return incident;
    }

    private Destinations destinations() {
        return new Destinations();
    }

    private Responders responders() {
        return new Responders();
    }
}
