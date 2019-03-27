package com.redhat.cajun.navy.process;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.drools.core.audit.event.LogEvent;
import org.drools.core.audit.event.RuleFlowNodeLogEvent;
import org.drools.core.command.runtime.process.SignalEventCommand;
import org.drools.core.command.runtime.process.StartCorrelatedProcessCommand;
import org.drools.core.command.runtime.process.StartProcessCommand;
import org.jbpm.executor.ExecutorServiceFactory;
import org.jbpm.executor.impl.wih.AsyncWorkItemHandler;
import org.jbpm.process.audit.JPAAuditLogService;
import org.jbpm.process.instance.event.DefaultSignalManagerFactory;
import org.jbpm.process.instance.impl.DefaultProcessInstanceManagerFactory;
import org.jbpm.runtime.manager.impl.DefaultRegisterableItemsFactory;
import org.jbpm.services.task.identity.JBossUserGroupCallbackImpl;
import org.jbpm.test.JbpmJUnitBaseTestCase;
import org.junit.After;
import org.kie.api.command.Command;
import org.kie.api.executor.ExecutorService;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.manager.RuntimeEngine;
import org.kie.api.runtime.manager.RuntimeEnvironmentBuilder;
import org.kie.api.runtime.manager.RuntimeManager;
import org.kie.api.runtime.manager.audit.AuditService;
import org.kie.api.runtime.manager.audit.NodeInstanceLog;
import org.kie.api.runtime.manager.audit.ProcessInstanceLog;
import org.kie.api.runtime.manager.audit.VariableInstanceLog;
import org.kie.api.runtime.process.ProcessInstance;
import org.kie.api.runtime.process.WorkItemHandler;
import org.kie.internal.KieInternalServices;
import org.kie.internal.io.ResourceFactory;
import org.kie.internal.process.CorrelationKey;
import org.kie.internal.process.CorrelationKeyFactory;
import org.kie.internal.runtime.manager.context.ProcessInstanceIdContext;

public class JbpmBaseTestCase extends JbpmJUnitBaseTestCase {

    private ExecutorService executorService;

    protected CorrelationKeyFactory correlationKeyFactory = KieInternalServices.Factory.get().newCorrelationKeyFactory();

    public JbpmBaseTestCase() {
        super();
    }

    public JbpmBaseTestCase(boolean setupDataSource, boolean sessionPersistence) {
        super(setupDataSource, sessionPersistence);
    }

    public JbpmBaseTestCase(boolean setupDataSource, boolean sessionPersistence, String persistenceUnitName) {
        super(setupDataSource, sessionPersistence, persistenceUnitName);
    }

    @After
    public void tearDown() throws Exception {
        if (executorService != null) {
            executorService.destroy();
            executorService = null;
        }
        super.tearDown();
    }

    protected Map<String, WorkItemHandler> getWorkItemHandlers() {
        Map<String, WorkItemHandler> workItemHandlers = new HashMap<String, WorkItemHandler>();
        return workItemHandlers;
    }

    protected RuntimeManager createRuntimeManager(Strategy strategy, String identifier, String... process) {
        return createRuntimeManager(strategy,  identifier, getWorkItemHandlers(), process);
    }

    protected RuntimeManager createRuntimeManager(Strategy strategy, String identifier, final Map<String, WorkItemHandler> workItemHandlers, String... process) {

        Map<String, ResourceType> resources = new HashMap<String, ResourceType>();
        for (String p : process) {
            resources.put(p, ResourceType.BPMN2);
        }

        if (manager != null) {
            throw new IllegalStateException("There is already one RuntimeManager active");
        }

        RuntimeEnvironmentBuilder builder = null;
        if (!setupDataSource){
            builder = RuntimeEnvironmentBuilder.Factory.get()
                    .newEmptyBuilder()
            .addConfiguration("drools.processSignalManagerFactory", DefaultSignalManagerFactory.class.getName())
            .addConfiguration("drools.processInstanceManagerFactory", DefaultProcessInstanceManagerFactory.class.getName());
        } else if (sessionPersistence) {
            builder = RuntimeEnvironmentBuilder.Factory.get()
                    .newDefaultBuilder()
            .entityManagerFactory(getEmf());
        } else {
            builder = RuntimeEnvironmentBuilder.Factory.get()
                    .newDefaultInMemoryBuilder();
        }
        builder.userGroupCallback(new JBossUserGroupCallbackImpl("classpath:/usergroups.properties"));

        for (Map.Entry<String, ResourceType> entry : resources.entrySet()) {
            builder.addAsset(ResourceFactory.newClassPathResource(entry.getKey()), entry.getValue());
        }

        builder.registerableItemsFactory(new DefaultRegisterableItemsFactory() {

            @Override
            public Map<String, WorkItemHandler> getWorkItemHandlers(RuntimeEngine runtime) {
                Map<String, WorkItemHandler> handlers = super.getWorkItemHandlers(runtime);
                for (Map.Entry<String, WorkItemHandler> entry : workItemHandlers.entrySet()) {
                    handlers.put(entry.getKey(), entry.getValue());
                }
                return handlers;
            }


        });
        return createRuntimeManager(strategy, resources, builder.get(), identifier);
    }

    protected String getProcessVarValue(long processInstanceId, String varName) {
        String actualValue = null;
        if (sessionPersistence) {
            getRuntimeEngine();
            List<? extends VariableInstanceLog> log = getLogService().findVariableInstances(processInstanceId, varName);
            if (log != null && !log.isEmpty()) {
                actualValue = log.get(log.size()-1).getValue();
            }

        } else {
            throw new IllegalStateException("No sessionpersistence");
        }
        return actualValue;
    }

    public void assertProcessInstanceCompleted(long processInstanceId) {
        assertTrue(assertProcessInstanceState(ProcessInstance.STATE_COMPLETED, processInstanceId));
    }

    public void assertProcessInstanceActive(long processInstanceId) {
        assertTrue(assertProcessInstanceState(ProcessInstance.STATE_ACTIVE, processInstanceId)
                || assertProcessInstanceState(ProcessInstance.STATE_PENDING, processInstanceId));
    }

    //delete?
    public void assertNodeTriggered(long processInstanceId, String... nodeNames) {
        List<String> names = new ArrayList<>();
        Collections.addAll(names, nodeNames);
        if (sessionPersistence) {
            List<? extends NodeInstanceLog> logs = getLogService().findNodeInstances(processInstanceId);
            if (logs != null) {
                for (NodeInstanceLog l : logs) {
                    String nodeName = l.getNodeName();
                    if ((l.getType() == NodeInstanceLog.TYPE_ENTER || l.getType() == NodeInstanceLog.TYPE_EXIT)) {
                        names.remove(nodeName);
                    }
                }
            }
        } else {
            for (LogEvent event : getInMemoryLogger().getLogEvents()) {
                if (event instanceof RuleFlowNodeLogEvent) {
                    String nodeName = ((RuleFlowNodeLogEvent) event).getNodeName();
                    names.remove(nodeName);
                }
            }
        }
        if (!names.isEmpty()) {
            StringBuilder s = new StringBuilder(names.get(0));
            for (int i = 1; i < names.size(); i++) {
                s.append(", ").append(names.get(i));
            }
            fail("Node(s) not triggered: " + s);
        }
    }

    public void assertNodeNotTriggered(long processInstanceId, String... nodeNames) {
        List<String> names = new ArrayList<>();
        List<String> triggered = new ArrayList<>();
        Collections.addAll(names, nodeNames);
        if (sessionPersistence) {
            List<? extends NodeInstanceLog> logs = getLogService().findNodeInstances(processInstanceId);
            if (logs != null) {
                for (NodeInstanceLog l : logs) {
                    String nodeName = l.getNodeName();
                    if ((l.getType() == NodeInstanceLog.TYPE_ENTER || l.getType() == NodeInstanceLog.TYPE_EXIT)) {
                        boolean removed = names.remove(nodeName);
                        if (removed) triggered.add(nodeName);
                    }
                }
            }
        } else {
            for (LogEvent event : getInMemoryLogger().getLogEvents()) {
                if (event instanceof RuleFlowNodeLogEvent) {
                    String nodeName = ((RuleFlowNodeLogEvent) event).getNodeName();
                    names.remove(nodeName);
                }
            }
        }
        if (!triggered.isEmpty()) {
            StringBuilder s = new StringBuilder(triggered.get(0));
            for (int i = 1; i < triggered.size(); i++) {
                s.append(", ").append(triggered.get(i));
            }
            fail("Node(s) triggered: " + s);
        }
    }

    protected boolean assertProcessInstanceState(int state, long processInstanceId) {
        if (sessionPersistence) {
            ProcessInstanceLog log = getLogService().findProcessInstance(processInstanceId);
            if (log != null) {
                return log.getStatus() == state;
            }
        } else {
            throw new IllegalStateException("No sessionpersistence");
        }

        return false;
    }

    protected void assertNodeActive(long processInstanceId, String... name) {
        List<String> names = new ArrayList<String>();
        for (String n : name) {
            names.add(n);
        }
        List<? extends NodeInstanceLog> logs = getLogService().findNodeInstances(processInstanceId);
        if (logs != null) {
            List<String> activeNodes = new ArrayList<String>();
            for (NodeInstanceLog l : logs) {
                String nodeName = l.getNodeName();
                if (l.getType() == NodeInstanceLog.TYPE_ENTER && names.contains(nodeName)) {
                    activeNodes.add(nodeName);
                }
                if (l.getType() == NodeInstanceLog.TYPE_EXIT && names.contains(nodeName)) {
                    activeNodes.remove(nodeName);
                }
            }
            names.removeAll(activeNodes);
        }


        if (!names.isEmpty()) {
            String s = names.get(0);
            for (int i = 1; i < names.size(); i++) {
                s += ", " + names.get(i);
            }
            fail("Node(s) not active: " + s);
        }
    }

    protected void assertNodeType(long processInstanceId, String name, String type) {

        List<? extends NodeInstanceLog> logs = getLogService().findNodeInstances(processInstanceId);
        if (logs != null) {
            List<String> activeNodes = new ArrayList<String>();
            for (NodeInstanceLog l : logs) {
                String nodeName = l.getNodeName();
                if (nodeName.equals(name)) {
                    if (!(l.getNodeType().equals(type))) {
                        fail("Node type for node " + name + " does not match. Expected: " + type + ", result: " + l.getNodeType());
                    }
                    return;
                }
            }
            fail("Node " + name + " is not active or triggered");
        }
    }

    protected long startProcess(RuntimeManager mgr, String processId) {
        return startProcess(mgr, processId, new HashMap<String, Object>());
    }

    @Override
    protected TestWorkItemHandler getTestWorkItemHandler() {
        return super.getTestWorkItemHandler();
    }

    protected long startProcess(RuntimeManager mgr, String processId, Map<String, Object> parameters) {
        StartProcessCommand startCmd = new StartProcessCommand(processId, parameters);
        return startProcess(mgr, startCmd);
    }

    protected long startProcess(RuntimeManager mgr, String processId, CorrelationKey correlationKey, Map<String, Object> parameters) {
        StartCorrelatedProcessCommand startCmd = new StartCorrelatedProcessCommand(processId, correlationKey, parameters);
        return startProcess(mgr, startCmd);
    }

    protected long startProcess(RuntimeManager mgr, Command<ProcessInstance> command) {
        RuntimeEngine runtimeEngine = getRuntimeEngine();
        KieSession session = runtimeEngine.getKieSession();
        ProcessInstance result = session.execute(command);
        long processInstanceId = result.getId();
        mgr.disposeRuntimeEngine(runtimeEngine);
        activeEngines.remove(runtimeEngine);
        return processInstanceId;
    }

    protected void signalProcess(RuntimeManager mgr, String type, Object event ) {
        SignalEventCommand signalCommand = new SignalEventCommand(type, event);
        signalProcess(mgr, signalCommand);
    }

    protected void signalProcess(RuntimeManager mgr, String type, Object event , long instanceId) {
        SignalEventCommand signalCommand = new SignalEventCommand(instanceId, type, event);
        signalProcess(mgr, signalCommand, instanceId);
    }

    protected void signalProcess(RuntimeManager mgr, Command<Void> command) {
        RuntimeEngine runtimeEngine = getRuntimeEngine();
        signalProcess(mgr, runtimeEngine, command);
    }

    protected void signalProcess(RuntimeManager mgr, Command<Void> command, long instanceId) {
        RuntimeEngine runtimeEngine = getRuntimeEngine(ProcessInstanceIdContext.get(instanceId));
        signalProcess(mgr, runtimeEngine, command);
    }

    protected void signalProcess(RuntimeManager mgr, RuntimeEngine runtimeEngine, Command<Void> command) {
        KieSession session = runtimeEngine.getKieSession();
        session.execute(command);
        mgr.disposeRuntimeEngine(runtimeEngine);
        activeEngines.remove(runtimeEngine);
    }

    @Override
    protected AuditService getLogService() {
        if (sessionPersistence) {
            return new JPAAuditLogService(getEmf());
        }
        return super.getLogService();
    }

    protected List<? extends ProcessInstanceLog> findActiveProcessInstances(String processId) {
        if (sessionPersistence) {
            return getLogService().findActiveProcessInstances(processId);
        } else {
            throw new IllegalStateException("No sessionpersistence");
        }
    }

    protected AsyncWorkItemHandler getAsynchWorkItemHandler() {
        executorService = ExecutorServiceFactory.newExecutorService(getEmf());
        executorService.init();
        return new AsyncWorkItemHandler(executorService);
    }

}
