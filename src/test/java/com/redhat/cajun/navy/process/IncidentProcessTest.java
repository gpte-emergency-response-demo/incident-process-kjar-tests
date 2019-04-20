package com.redhat.cajun.navy.process;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.arjuna.ats.arjuna.coordinator.TxControl;
import com.redhat.cajun.navy.rules.model.Destinations;
import com.redhat.cajun.navy.rules.model.Incident;
import com.redhat.cajun.navy.rules.model.IncidentPriority;
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

    private List<Map<String, Object>> incidentPriorityServiceWihParameters = new ArrayList<>();

    private String incidentId;

    private String responderId;

    private Destinations destinations;

    private Responders responders;

    private IncidentPriority incidentPriority;

    private int nrAssignments = 0;

    public IncidentProcessTest() {
        super(true, true);
    }

    @BeforeClass
    public static void setupTest() {
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
     *      The IncidentPriorityService wih is invoked to get the priority of the incident
     *      The BusinessRuleTask wih is invoked to assign a mission
     *      The SendMessage wih is invoked which sends a SetResponderUnavailable message to Kafka
     *      The process is waiting on a signal with reference ResponderAvailable
     */
    @Test
    public void testIncidentProcess() {

        setup(true);

        Incident incident = incident(incidentId);

        long pId = startProcess(incident, destinations, "PT60S");

        assertProcessInstanceActive(pId);
        assertNodeTriggered(pId, "Get Active Responders", "Get Incident Priority", "Assign Mission", "Update Responder Availability");
        assertNodeActive(pId, "signal1");

        verify(workItemHandlers.get("ResponderService")).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));
        verify(workItemHandlers.get("IncidentPriorityService")).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));
        verify(workItemHandlers.get("BusinessRuleTask")).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));
        verify(workItemHandlers.get("SendMessage")).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));

        // IncidentPriorityService
        Map<String, Object> params = incidentPriorityServiceWihParameters.get(0);
        assertThat(params, notNullValue());
        assertThat(params.get("Incident"), equalTo(incident));

        // BusinessRuleTask
        params = businessRuleTaskParameters.get(0);
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
        assertThat(params.get("IncidentPriority"), notNullValue());
        assertThat(params.get("IncidentPriority"), is(instanceOf(IncidentPriority.class)));
        IncidentPriority incidentPriority = (IncidentPriority)params.get("IncidentPriority");
        assertThat(incidentPriority.getIncidentId(), equalTo(incidentId));

        // SendMessageTask
        params = sendMessageWihParameters.get(0);
        assertThat(params, notNullValue());
        assertThat(params.get("MessageType"), equalTo("SetResponderUnavailable"));
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
     *        (the process is signaled with a signal with reference ResponderAvailable and payload true)
     *    Then:
     *      The SendMessage wih is invoked which sends a IncidentAssignmentEvent to Kafka
     *      The SendMessage wih is invoked which sends a CreateMission message to Kafka
     *      The process is waiting on a signal with reference MissionCreated.
     */
    @Test
    public void testIncidentProcessWhenSignalResponderAvailableEvent() {

        setup(true);

        Incident incident = incident(incidentId);

        long pId = startProcess(incident, destinations, "PT60S");

        // Signal process
        signalProcess(mgr, "ResponderAvailable", Boolean.TRUE, pId);

        assertProcessInstanceActive(pId);

        assertNodeTriggered(pId, "Create Mission Command");

        verify(workItemHandlers.get("SendMessage"), times(3)).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));

        // SendMessageTask
        Map<String, Object>  params = sendMessageWihParameters.get(1);
        assertThat(params, notNullValue());
        assertThat(params.get("MessageType"), equalTo("IncidentAssignment"));
        assertThat(params.get("Payload"), notNullValue());
        assertThat(params.get("Payload"), is(instanceOf(Mission.class)));
        Mission m = (Mission) params.get("Payload");
        assertThat(m.getStatus(), equalTo(Status.ASSIGNED));
        assertThat(m.getIncidentId(), equalTo(incidentId));

        params = sendMessageWihParameters.get(2);
        assertThat(params, notNullValue());
        assertThat(params.get("MessageType"), equalTo("CreateMission"));
        assertThat(params.get("Payload"), notNullValue());
        assertThat(params.get("Payload"), is(instanceOf(Mission.class)));
        m = (Mission) params.get("Payload");
        assertThat(m.getStatus(), equalTo(Status.ASSIGNED));
        assertThat(m.getIncidentId(), equalTo(incidentId));
        assertThat(m.getResponderId(), equalTo(responderId));

        assertNodeActive(pId, "signal2");

    }

    /**
     *  Test description:
     *    Given:
     *    When :
     *      an instance of the incident process is started
     *      a mission cannot be assigned to the incident
     *    Then:
     *      The ResponderService wih is invoked to get the List of available Responders
     *      The IncidentPriorityService wih is invoked to get the priority of the incident
     *      The BusinessRuleTask wih is invoked to assign a mission
     *      The SendMessage wih is invoked which sends a IncidentAssignmentEvent to Kafka
     *      The process waits in a timer node
     */
    @Test
    public void testIncidentProcessWhenNoAssignment() {
        setup(false);

        Incident incident = incident(incidentId);

        long pId = startProcess(incident, destinations, "PT60M");

        assertProcessInstanceActive(pId);
        assertNodeTriggered(pId, "Get Active Responders", "Get Incident Priority", "Assign Mission", "Incident Assignment Event");
        assertNodeNotTriggered(pId, "Verify Responder Available command");
        assertNodeActive(pId, "timer");

        verify(workItemHandlers.get("ResponderService")).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));
        verify(workItemHandlers.get("IncidentPriorityService")).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));
        verify(workItemHandlers.get("BusinessRuleTask")).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));
        verify(workItemHandlers.get("SendMessage")).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));

        // IncidentPriorityService
        Map<String, Object> params = incidentPriorityServiceWihParameters.get(0);
        assertThat(params, notNullValue());
        assertThat(params.get("Incident"), equalTo(incident));

        // BusinessRuleTask
        params = businessRuleTaskParameters.get(0);
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
        assertThat(params.get("MessageType"), equalTo("IncidentAssignment"));
        assertThat(params.get("Payload"), notNullValue());
        assertThat(params.get("Payload"), is(instanceOf(Mission.class)));
        Mission m = (Mission) params.get("Payload");
        assertThat(m.getStatus(), equalTo(Status.UNASSIGNED));
        assertThat(m.getIncidentId(), equalTo(incidentId));
        assertThat(m.getResponderStartLat(), nullValue());
        assertThat(m.getResponderStartLong(), nullValue());
        assertThat(m.getDestinationLat(), nullValue());
        assertThat(m.getDestinationLong(), nullValue());
        assertThat(m.getResponderId(), nullValue());
    }

    /**
     *  Test description:
     *    Given:
     *    When :
     *      an instance of the incident process is started
     *      a mission can be assigned to the incident
     *      the responder assigned to the mission is not available
     *        (the process is signaled with a signal with reference ResponderAvailable and payload false)
     *    Then:
     *      The ResponderService wih is invoked to get the List of available Responders
     *      The IncidentPriorityService wih is invoked to get the priority of the incident
     *      The BusinessRuleTask wih is invoked to assign a mission
     *      The SendMessage wih is invoked which sends a SetResponderUnavailable message to Kafka
     *      The SendMessage wih is invoked which send a IncidentAssignmentEvent to Kafka
     *      The process waits in a timer node
     */
    @Test
    public void testIncidentProcessWhenResponderNotAvailable() {
        setup(true);

        Incident incident = incident(incidentId);

        long pId = startProcess(incident, destinations, "PT60M");

        // Signal process
        signalProcess(mgr, "ResponderAvailable", Boolean.FALSE, pId);

        assertProcessInstanceActive(pId);
        assertNodeNotTriggered(pId, "Create Mission Command");

        verify(workItemHandlers.get("SendMessage"), times(2)).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));

        assertNodeActive(pId, "timer");

        // SendMessageTask
        Map<String, Object> params = sendMessageWihParameters.get(1);
        assertThat(params, notNullValue());
        assertThat(params.get("MessageType"), equalTo("IncidentAssignment"));
        assertThat(params.get("Payload"), notNullValue());
        assertThat(params.get("Payload"), is(instanceOf(Mission.class)));
        Mission m = (Mission) params.get("Payload");
        assertThat(m.getStatus(), equalTo(Status.UNASSIGNED));
        assertThat(m.getIncidentId(), equalTo(incidentId));
        assertThat(m.getResponderStartLat(), nullValue());
        assertThat(m.getResponderStartLong(), nullValue());
        assertThat(m.getDestinationLat(), nullValue());
        assertThat(m.getDestinationLong(), nullValue());
        assertThat(m.getResponderId(), nullValue());
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
     *      The IncidentPriorityService wih is invoked twice to get the priority of the incident
     *      The BusinessRuleTask wih is invoked twice to assign a mission
     *      The SendMessage wih is invoked which send a IncidentAssignmentEvent to Kafka
     *      The SendMessage wih is invoked which sends a SetResponderUnavailable message to Kafka
     *      The process is waiting on a signal with reference ResponderAvailable
     */
    @Test
    public void testIncidentProcessSecondAssignment() throws Exception {

        setup(false);

        Incident incident = incident(incidentId);

        long pId = startProcess(incident, destinations, "PT1S");

        //wait for timer to fire
        Thread.sleep(5000);

        assertProcessInstanceActive(pId);
        assertNodeTriggered(pId, "Get Active Responders", "Assign Mission", "Update Responder Availability", "timer");
        assertNodeActive(pId, "signal1");

        verify(workItemHandlers.get("ResponderService"), times(2)).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));
        verify(workItemHandlers.get("BusinessRuleTask"), times(2)).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));
        verify(workItemHandlers.get("IncidentPriorityService"), times(2)).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));
        verify(workItemHandlers.get("SendMessage"), times(2)).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));

        // SendMessageTask
        Map<String, Object> params = sendMessageWihParameters.get(0);
        assertThat(params, notNullValue());
        assertThat(params.get("MessageType"), equalTo("IncidentAssignment"));
        assertThat(params.get("Payload"), notNullValue());
        assertThat(params.get("Payload"), is(instanceOf(Mission.class)));
        Mission m = (Mission) params.get("Payload");
        assertThat(m.getStatus(), equalTo(Status.UNASSIGNED));
        assertThat(m.getIncidentId(), equalTo(incidentId));
    }

    /**
     *  Test description:
     *    Given:
     *    When :
     *      an instance of the incident process is started
     *      a mission can be assigned to the incident
     *      the responder assigned to the mission is available
     *        (the process instance is signaled with a signal with reference ResponderAvailable and payload true)
     *      the mission is started
     *        (the process instance is signaled with a signal with reference MissionStarted
     *    Then:
     *      The status of the incident is set to 'Assigned'
     *      The SendMessage wih is invoked which sends a UpdateIncident message to Kafka
     *      The process is waiting on a signal with reference VictimPickedUp.
     */
    @Test
    public void testIncidentProcessWhenMissionStartedSignal() {

        setup(true);

        Incident incident = incident(incidentId);

        long pId = startProcess(incident, destinations, "PT60S");

        // Signal process ResponderAvailable
        signalProcess(mgr, "ResponderAvailable", Boolean.TRUE, pId);

        // Signal process MissionStarted
        signalProcess(mgr, "MissionStarted",null, pId);

        assertProcessInstanceActive(pId);

        assertNodeTriggered(pId, "Update Incident Assigned");

        verify(workItemHandlers.get("SendMessage"), times(4)).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));

        // SendMessageTask
        Map<String, Object> params = sendMessageWihParameters.get(3);
        assertThat(params, notNullValue());
        assertThat(params.get("MessageType"), equalTo("UpdateIncident"));
        assertThat(params.get("Payload"), notNullValue());
        assertThat(params.get("Payload"), is(instanceOf(Incident.class)));
        Incident payload = (Incident) params.get("Payload");
        assertThat(payload.getId(), equalTo(incidentId));
        assertThat(payload.getStatus(), equalTo("Assigned"));

        assertNodeActive(pId, "signal3");
    }

    /**
     *  Test description:
     *    Given:
     *    When :
     *      an instance of the incident process is started
     *      a mission can be assigned to the incident
     *      the responder assigned to the mission is available
     *        (the process instance is signaled with a signal with reference ResponderAvailable and payload true)
     *      the mission is aborted
     *        (the process instance is signaled with a signal with reference MissionAborted
     *    Then:
     *      The status of the incident is set to 'Aborted'
     *      The SendMessage wih is invoked which sends a UpdateIncident message to Kafka
     *      The process is completed.
     */
    @Test
    public void testIncidentProcessMissionAbortedBeforeMissionStartedSignal() {

        setup(true);

        Incident incident = incident(incidentId);

        long pId = startProcess(incident, destinations, "PT60S");

        // Signal process ResponderAvailable
        signalProcess(mgr, "ResponderAvailable", Boolean.TRUE, pId);

        // Signal process MissionAborted
        signalProcess(mgr, "MissionAborted", null, pId);

        assertProcessInstanceCompleted(pId);

        assertNodeTriggered(pId, "Mission Aborted", "Update Incident Aborted");
        assertNodeNotTriggered(pId, "Update Incident Assigned");

        verify(workItemHandlers.get("SendMessage"), times(4)).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));

        // SendMessageTask
        Map<String, Object> params = sendMessageWihParameters.get(3);
        assertThat(params, notNullValue());
        assertThat(params.get("MessageType"), equalTo("UpdateIncident"));
        assertThat(params.get("Payload"), notNullValue());
        assertThat(params.get("Payload"), is(instanceOf(Incident.class)));
        Incident payload = (Incident) params.get("Payload");
        assertThat(payload.getId(), equalTo(incidentId));
        assertThat(payload.getStatus(), equalTo("Aborted"));
    }

    /**
     *  Test description:
     *    Given:
     *    When :
     *      an instance of the incident process is started
     *      a mission can be assigned to the incident
     *      the responder assigned to the mission is available
     *        (the process instance is signaled with a signal with reference ResponderAvailable and payload true)
     *      the mission is started
     *        (the process instance is signaled with a signal with reference MissionStarted
     *      the victim is picked up
     *        (the process instance is signaled with a signal with reference VictimPickedUp
     *    Then:
     *      The status of the incident is set to 'PickedUp'
     *      The SendMessage wih is invoked which sends a UpdateIncident message to Kafka
     *      The process is waiting on a signal with reference VictimDelivered.
     */
    @Test
    public void testIncidentProcessWhenVictimPickedUpSignal() {

        setup(true);

        Incident incident = incident(incidentId);

        long pId = startProcess(incident, destinations, "PT60S");

        // Signal process ResponderAvailable
        signalProcess(mgr, "ResponderAvailable", Boolean.TRUE, pId);

        // Signal process MissionStarted
        signalProcess(mgr, "MissionStarted",null, pId);

        // Signal process VictimPickedUp
        signalProcess(mgr, "VictimPickedUp", null, pId);

        assertProcessInstanceActive(pId);

        assertNodeTriggered(pId, "Update Incident PickedUp");

        verify(workItemHandlers.get("SendMessage"), times(5)).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));

        // SendMessageTask
        Map<String, Object> params = sendMessageWihParameters.get(4);
        assertThat(params, notNullValue());
        assertThat(params.get("MessageType"), equalTo("UpdateIncident"));
        assertThat(params.get("Payload"), notNullValue());
        assertThat(params.get("Payload"), is(instanceOf(Incident.class)));
        Incident payload = (Incident) params.get("Payload");
        assertThat(payload.getId(), equalTo(incidentId));
        assertThat(payload.getStatus(), equalTo("PickedUp"));

        assertNodeActive(pId, "signal4");
    }

    /**
     *  Test description:
     *    Given:
     *    When :
     *      an instance of the incident process is started
     *      a mission can be assigned to the incident
     *      the responder assigned to the mission is available
     *        (the process instance is signaled with a signal with reference ResponderAvailable and payload true)
     *      the mission is started
     *        (the process instance is signaled with a signal with reference MissionStarted
     *      the mission is aborted
     *        (the process instance is signaled with a signal with reference MissionAborted
     *    Then:
     *      The status of the incident is set to 'Aborted'
     *      The SendMessage wih is invoked which sends a UpdateIncident message to Kafka
     *      The process is completed.
     */
    @Test
    public void testIncidentProcessMissionAbortedBeforeVictimPickedSignal() {

        setup(true);

        Incident incident = incident(incidentId);

        long pId = startProcess(incident, destinations, "PT60S");

        // Signal process ResponderAvailable
        signalProcess(mgr, "ResponderAvailable", Boolean.TRUE, pId);

        // Signal process MissionStarted
        signalProcess(mgr, "MissionStarted",null, pId);

        // Signal process MissionAborted
        signalProcess(mgr, "MissionAborted", null, pId);

        assertProcessInstanceCompleted(pId);

        assertNodeTriggered(pId, "Update Incident Assigned", "Mission Aborted", "Update Incident Aborted");
        assertNodeNotTriggered(pId, "Update Incident PickedUp");

        verify(workItemHandlers.get("SendMessage"), times(5)).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));

        // SendMessageTask
        Map<String, Object> params = sendMessageWihParameters.get(4);
        assertThat(params, notNullValue());
        assertThat(params.get("MessageType"), equalTo("UpdateIncident"));
        assertThat(params.get("Payload"), notNullValue());
        assertThat(params.get("Payload"), is(instanceOf(Incident.class)));
        Incident payload = (Incident) params.get("Payload");
        assertThat(payload.getId(), equalTo(incidentId));
        assertThat(payload.getStatus(), equalTo("Aborted"));
    }

    /**
     *  Test description:
     *    Given:
     *    When :
     *      an instance of the incident process is started
     *      a mission can be assigned to the incident
     *      the responder assigned to the mission is available
     *        (the process instance is signaled with a signal with reference ResponderAvailable and payload true)
     *      the mission is started
     *        (the process instance is signaled with a signal with reference MissionStarted
     *      the victim is picked up
     *        (the process instance is signaled with a signal with reference VictimPickedUp
     *      the victim is delivered
     *        (the process instance is signaled with a signal with reference VictimDelivered
     *    Then:
     *      The status of the incident is set to 'Delivered'
     *      The SendMessage wih is invoked which sends a UpdateIncident message to Kafka
     *      The process is completed.
     */
    @Test
    public void testIncidentProcessWhenVictimDeliveredSignal() {

        setup(true);

        Incident incident = incident(incidentId);

        long pId = startProcess(incident, destinations, "PT60S");

        // Signal process ResponderAvailable
        signalProcess(mgr, "ResponderAvailable", Boolean.TRUE, pId);

        // Signal process MissionStarted
        signalProcess(mgr, "MissionStarted",null, pId);

        // Signal process VictimPickedUp
        signalProcess(mgr, "VictimPickedUp", null, pId);

        // Signal process VictimDelivered
        signalProcess(mgr, "VictimDelivered", null, pId);

        assertProcessInstanceCompleted(pId);

        assertNodeTriggered(pId, "Update Incident Delivered");

        verify(workItemHandlers.get("SendMessage"), times(6)).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));

        // SendMessageTask
        Map<String, Object> params = sendMessageWihParameters.get(5);
        assertThat(params, notNullValue());
        assertThat(params.get("MessageType"), equalTo("UpdateIncident"));
        assertThat(params.get("Payload"), notNullValue());
        assertThat(params.get("Payload"), is(instanceOf(Incident.class)));
        Incident payload = (Incident) params.get("Payload");
        assertThat(payload.getId(), equalTo(incidentId));
        assertThat(payload.getStatus(), equalTo("Delivered"));
    }

    /**
     *  Test description:
     *    Given:
     *    When :
     *      an instance of the incident process is started
     *      a mission can be assigned to the incident
     *      the responder assigned to the mission is available
     *        (the process instance is signaled with a signal with reference ResponderAvailable and payload true)
     *      the mission is started
     *        (the process instance is signaled with a signal with reference MissionStarted
     *      the victim is picked up
     *        (the process instance is signaled with a signal with reference VictimPickedUp
     *      the mission is aborted
     *        (the process instance is signaled with a signal with reference MissionAborted
     *    Then:
     *      The status of the incident is set to 'Aborted'
     *      The SendMessage wih is invoked which sends a UpdateIncident message to Kafka
     *      The process is completed.
     */
    @Test
    public void testIncidentProcessMissionAbortedBeforeVictimDeliveredSignal() {

        setup(true);

        Incident incident = incident(incidentId);

        long pId = startProcess(incident, destinations, "PT60S");

        // Signal process ResponderAvailable
        // Signal process ResponderAvailable
        signalProcess(mgr, "ResponderAvailable", Boolean.TRUE, pId);

        // Signal process MissionStarted
        signalProcess(mgr, "MissionStarted",null, pId);

        // Signal process VictimPickedUp
        signalProcess(mgr, "VictimPickedUp", null, pId);

        // Signal process MissionAborted
        signalProcess(mgr, "MissionAborted", null, pId);

        assertProcessInstanceCompleted(pId);

        assertNodeTriggered(pId, "Update Incident PickedUp", "Mission Aborted", "Update Incident Aborted");
        assertNodeNotTriggered(pId, "Update Incident Delivered");

        verify(workItemHandlers.get("SendMessage"), times(6)).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));

        // SendMessageTask
        Map<String, Object> params = sendMessageWihParameters.get(5);
        assertThat(params, notNullValue());
        assertThat(params.get("MessageType"), equalTo("UpdateIncident"));
        assertThat(params.get("Payload"), notNullValue());
        assertThat(params.get("Payload"), is(instanceOf(Incident.class)));
        Incident payload = (Incident) params.get("Payload");
        assertThat(payload.getId(), equalTo(incidentId));
        assertThat(payload.getStatus(), equalTo("Aborted"));
    }

    private void setup(boolean assigned) {

        incidentId = UUID.randomUUID().toString();
        responderId = "responderId";
        responders = responders();
        destinations = destinations();
        incidentPriority = incidentPriority(incidentId);

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

            WorkItemHandler mockIncidentPriorityServiceWih = mock(WorkItemHandler.class);
            workItemHandlers.put("IncidentPriorityService", mockIncidentPriorityServiceWih);
            doAnswer(invocation -> {
                WorkItem workItem = (WorkItem) invocation.getArguments()[0];
                WorkItemManager workItemManager = (WorkItemManager) invocation.getArguments()[1];
                incidentPriorityServiceWihParameters.add(workItem.getParameters());
                Map<String, Object> results = new HashMap<>();
                results.put("IncidentPriority", incidentPriority);
                workItemManager.completeWorkItem(workItem.getId(), results);
                return null;
            }).when(mockIncidentPriorityServiceWih).executeWorkItem(any(WorkItem.class), any(WorkItemManager.class));

            mgr = createRuntimeManager(Strategy.PROCESS_INSTANCE, "test", workItemHandlers, "com/redhat/cajun/navy/process/incident-process.bpmn");
        }
    }

    private Mission businessRuleTaskResult(boolean assigned) {
        Mission mission = new Mission();
        mission.setIncidentId(incidentId);
        if (assigned) {
            mission.setStatus(Status.ASSIGNED);
            mission.setResponderId(responderId);
            mission.setResponderStartLat(new BigDecimal("30.12345"));
            mission.setResponderStartLong(new BigDecimal("-77.98765"));
            mission.setDestinationLat(new BigDecimal("31.98765"));
            mission.setDestinationLong(new BigDecimal("-78.13579"));
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

    private IncidentPriority incidentPriority(String incidentId) {
        IncidentPriority ip = new IncidentPriority();
        ip.setIncidentId(incidentId);
        return ip;
    }
}
