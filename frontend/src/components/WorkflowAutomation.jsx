import { useState, useEffect } from 'react'
import { Button } from '@/components/ui/button.jsx'
import { Input } from '@/components/ui/input.jsx'
import { Textarea } from '@/components/ui/textarea.jsx'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select.jsx'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card.jsx'
import { Badge } from '@/components/ui/badge.jsx'
import { Switch } from '@/components/ui/switch.jsx'
import { Progress } from '@/components/ui/progress.jsx'
import { Workflow, Plus, Play, Pause, MoreVertical, Clock, CheckCircle, XCircle, Settings, Zap, Trash2 } from 'lucide-react'

const WorkflowAutomation = () => {
  const [workflows, setWorkflows] = useState([])
  const [workflowRuns, setWorkflowRuns] = useState([])
  const [newWorkflow, setNewWorkflow] = useState({
    name: '',
    description: '',
    trigger_type: 'manual',
    trigger_config: '',
    steps: '',
    enabled: true
  })
  const [selectedWorkflow, setSelectedWorkflow] = useState('')
  const [isCreating, setIsCreating] = useState(false)

  const triggerTypes = [
    { value: 'manual', label: 'Manual Trigger', description: 'Start workflow manually' },
    { value: 'job_completed', label: 'Job Completed', description: 'Trigger when a job completes' },
    { value: 'confidence_threshold', label: 'Confidence Threshold', description: 'Trigger when confidence is too low' },
    { value: 'label_distribution', label: 'Label Distribution', description: 'Trigger based on label patterns' },
    { value: 'error_rate', label: 'Error Rate', description: 'Trigger when error rate is high' },
    { value: 'processing_time', label: 'Processing Time', description: 'Trigger based on processing time' },
    { value: 'schedule', label: 'Scheduled', description: 'Run on a schedule' }
  ]

  const stepTypes = [
    { value: 'email_notification', label: 'Email Notification' },
    { value: 'webhook', label: 'Webhook' },
    { value: 'export_data', label: 'Export Data' },
    { value: 'retrain_model', label: 'Retrain Model' },
    { value: 'archive_data', label: 'Archive Data' },
    { value: 'generate_report', label: 'Generate Report' },
    { value: 'escalate_review', label: 'Escalate Review' },
    { value: 'update_template', label: 'Update Template' }
  ]

  useEffect(() => {
    fetchWorkflows()
    fetchWorkflowRuns()
  }, [])

  const fetchWorkflows = async () => {
    try {
      const response = await fetch('http://localhost:8000/api/v1/workflows/')
      if (response.ok) {
        const data = await response.json()
        setWorkflows(data.workflows || [])
      }
    } catch (error) {
      console.error('Error fetching workflows:', error)
    }
  }

  const fetchWorkflowRuns = async () => {
    try {
      const response = await fetch('http://localhost:8000/api/v1/workflows/runs')
      if (response.ok) {
        const data = await response.json()
        setWorkflowRuns(data.runs || [])
      }
    } catch (error) {
      console.error('Error fetching workflow runs:', error)
    }
  }

  const createWorkflow = async () => {
    if (!newWorkflow.name.trim() || !newWorkflow.steps.trim()) {
      alert('Please provide workflow name and steps configuration')
      return
    }

    setIsCreating(true)
    try {
      let triggerConfig = {}
      let steps = []

      if (newWorkflow.trigger_config.trim()) {
        try {
          triggerConfig = JSON.parse(newWorkflow.trigger_config)
        } catch {
          alert('Invalid JSON in trigger configuration')
          setIsCreating(false)
          return
        }
      }

      try {
        steps = JSON.parse(newWorkflow.steps)
      } catch {
        alert('Invalid JSON in steps configuration')
        setIsCreating(false)
        return
      }

      // Convert frontend format to backend format
      const workflowData = {
        name: newWorkflow.name,
        description: newWorkflow.description,
        triggers: [
          {
            type: newWorkflow.trigger_type,
            conditions: triggerConfig,
            metadata: {}
          }
        ],
        actions: steps.map((step, index) => ({
          type: step.type,
          parameters: step.config || {},
          order: index,
          max_retries: 3
        })),
        is_active: newWorkflow.enabled
      }

      const response = await fetch('http://localhost:8000/api/v1/workflows/', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(workflowData)
      })

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`)
      }

      const result = await response.json()
      alert('Workflow created successfully!')
      
      // Reset form
      setNewWorkflow({
        name: '',
        description: '',
        trigger_type: 'manual',
        trigger_config: '',
        steps: '',
        enabled: true
      })
      
      // Refresh workflows
      fetchWorkflows()
    } catch (error) {
      console.error('Error creating workflow:', error)
      alert('Error creating workflow: ' + error.message)
    } finally {
      setIsCreating(false)
    }
  }

  const executeWorkflow = async (workflowId, parameters = {}) => {
    try {
      const response = await fetch(`http://localhost:8000/api/v1/workflows/trigger`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ 
          trigger_type: 'manual',
          data: { workflow_id: workflowId, ...parameters }
        })
      })

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`)
      }

      const result = await response.json()
      alert('Workflow execution started!\nTriggered workflows: ' + result.triggered_workflows)
      
      // Refresh workflow runs
      fetchWorkflowRuns()
    } catch (error) {
      console.error('Error executing workflow:', error)
      alert('Error executing workflow: ' + error.message)
    }
  }

  const toggleWorkflow = async (workflowId, enabled) => {
    try {
      // Since there's no update endpoint in the backend, we'll need to implement this
      // For now, just update the local state
      setWorkflows(prev => prev.map(workflow => 
        workflow.id === workflowId ? { ...workflow, is_active: enabled } : workflow
      ))
      
      // Note: Backend doesn't have an update endpoint yet
      console.log(`Workflow ${workflowId} ${enabled ? 'enabled' : 'disabled'} (local only)`)
    } catch (error) {
      console.error('Error toggling workflow:', error)
    }
  }

  const deleteWorkflow = async (workflowId, workflowName) => {
    if (!confirm(`Are you sure you want to delete the workflow "${workflowName}"? This action cannot be undone.`)) {
      return
    }

    try {
      const response = await fetch(`http://localhost:8000/api/v1/workflows/${workflowId}`, {
        method: 'DELETE',
        headers: {
          'Content-Type': 'application/json',
        }
      })

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`)
      }

      const result = await response.json()
      alert('Workflow deleted successfully!')
      
      // Refresh workflows
      fetchWorkflows()
    } catch (error) {
      console.error('Error deleting workflow:', error)
      alert('Error deleting workflow: ' + error.message)
    }
  }

  const getStatusColor = (status) => {
    switch (status) {
      case 'completed': return 'default'
      case 'running': return 'secondary'
      case 'failed': return 'destructive'
      case 'pending': return 'outline'
      default: return 'outline'
    }
  }

  const getStatusIcon = (status) => {
    switch (status) {
      case 'completed': return <CheckCircle className="w-4 h-4" />
      case 'running': return <Clock className="w-4 h-4" />
      case 'failed': return <XCircle className="w-4 h-4" />
      default: return <Clock className="w-4 h-4" />
    }
  }

  return (
    <div className="space-y-6">
      <div className="text-center py-6">
        <Workflow className="w-12 h-12 mx-auto text-indigo-500 mb-4" />
        <h2 className="text-2xl font-bold text-gray-900 mb-2">Workflow Automation</h2>
        <p className="text-gray-600 max-w-2xl mx-auto">
          Create and manage automated workflows for your data labeling processes
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Create Workflow */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Plus className="w-5 h-5" />
              Create Workflow
            </CardTitle>
            <CardDescription>
              Design a new automated workflow
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <label className="block text-sm font-medium mb-2">Workflow Name</label>
              <Input
                value={newWorkflow.name}
                onChange={(e) => setNewWorkflow(prev => ({ ...prev, name: e.target.value }))}
                placeholder="Enter workflow name"
              />
            </div>

            <div>
              <label className="block text-sm font-medium mb-2">Description</label>
              <Textarea
                value={newWorkflow.description}
                onChange={(e) => setNewWorkflow(prev => ({ ...prev, description: e.target.value }))}
                placeholder="Describe what this workflow does"
                rows={2}
              />
            </div>

            <div>
              <label className="block text-sm font-medium mb-2">Trigger Type</label>
              <Select 
                value={newWorkflow.trigger_type} 
                onValueChange={(value) => setNewWorkflow(prev => ({ ...prev, trigger_type: value }))}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Select trigger type" />
                </SelectTrigger>
                <SelectContent>
                  {triggerTypes.map((trigger) => (
                    <SelectItem key={trigger.value} value={trigger.value}>
                      <div>
                        <div className="font-medium">{trigger.label}</div>
                        <div className="text-xs text-gray-500">{trigger.description}</div>
                      </div>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div>
              <label className="block text-sm font-medium mb-2">Trigger Configuration (JSON)</label>
              <Textarea
                value={newWorkflow.trigger_config}
                onChange={(e) => setNewWorkflow(prev => ({ ...prev, trigger_config: e.target.value }))}
                placeholder={
                  newWorkflow.trigger_type === 'confidence_threshold' ? '{"avg_confidence": {"value": 0.8, "operator": "less_than"}}' :
                  newWorkflow.trigger_type === 'job_completed' ? '{"job_status": {"value": "completed", "operator": "equals"}}' :
                  newWorkflow.trigger_type === 'error_rate' ? '{"error_rate": {"value": 0.1, "operator": "greater_than"}}' :
                  '{}'
                }
                rows={3}
                className="font-mono text-sm"
              />
            </div>

            <div>
              <label className="block text-sm font-medium mb-2">Workflow Steps (JSON)</label>
              <Textarea
                value={newWorkflow.steps}
                onChange={(e) => setNewWorkflow(prev => ({ ...prev, steps: e.target.value }))}
                placeholder={`[
  {
    "type": "email_notification",
    "config": {"to": ["admin@example.com"], "subject": "Alert"}
  },
  {
    "type": "generate_report",
    "config": {"format": "pdf", "include_charts": true}
  }
]`}
                rows={8}
                className="font-mono text-sm"
              />
            </div>

            <div className="flex items-center space-x-2">
              <Switch
                checked={newWorkflow.enabled}
                onCheckedChange={(checked) => setNewWorkflow(prev => ({ ...prev, enabled: checked }))}
              />
              <label className="text-sm font-medium">Enable workflow</label>
            </div>

            <Button onClick={createWorkflow} disabled={isCreating} className="w-full">
              {isCreating ? 'Creating...' : 'Create Workflow'}
            </Button>
          </CardContent>
        </Card>

        {/* Workflow Management */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Settings className="w-5 h-5" />
              Quick Actions
            </CardTitle>
            <CardDescription>
              Execute workflows and view templates
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <label className="block text-sm font-medium mb-2">Execute Workflow</label>
              <Select value={selectedWorkflow} onValueChange={setSelectedWorkflow}>
                <SelectTrigger>
                  <SelectValue placeholder="Select workflow to execute" />
                </SelectTrigger>
                <SelectContent>
                  {workflows.filter(w => w.is_active).map((workflow) => (
                    <SelectItem key={workflow.id} value={workflow.id}>
                      {workflow.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <Button 
              onClick={() => executeWorkflow(selectedWorkflow)}
              disabled={!selectedWorkflow}
              className="w-full"
            >
              <Play className="w-4 h-4 mr-2" />
              Execute Workflow
            </Button>

            <div className="border-t pt-4">
              <h4 className="font-medium mb-2">Workflow Templates</h4>
              <div className="space-y-2">
                {stepTypes.map((step) => (
                  <div key={step.value} className="flex items-center gap-2 text-sm p-2 bg-gray-50 rounded">
                    <Zap className="w-3 h-3 text-gray-400" />
                    <span>{step.label}</span>
                  </div>
                ))}
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Workflows List */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Workflow className="w-5 h-5" />
            Configured Workflows
          </CardTitle>
          <CardDescription>
            Manage your automated workflows
          </CardDescription>
        </CardHeader>
        <CardContent>
          {workflows.length === 0 ? (
            <div className="text-center py-8 text-gray-500">
              <Workflow className="w-8 h-8 mx-auto mb-2 opacity-50" />
              <p>No workflows configured. Create your first workflow above.</p>
            </div>
          ) : (
            <div className="space-y-4">
              {workflows.map((workflow) => (
                <div key={workflow.id} className="border rounded-lg p-4">
                  <div className="flex justify-between items-start mb-3">
                    <div className="flex-1">
                      <div className="flex items-center gap-2 mb-1">
                        <h4 className="font-medium">{workflow.name}</h4>
                        <Badge variant={workflow.is_active ? 'default' : 'secondary'}>
                          {workflow.is_active ? 'Enabled' : 'Disabled'}
                        </Badge>
                        <Badge variant="outline">
                          {workflow.triggers?.[0]?.type || 'No trigger'}
                        </Badge>
                      </div>
                      <div className="text-sm text-gray-600 mb-2">
                        {workflow.description}
                      </div>
                      <div className="text-xs text-gray-500">
                        Actions: {workflow.actions?.length || 0} | 
                        Created: {new Date(workflow.created_at).toLocaleDateString()}
                        {workflow.last_executed && (
                          <span> | Last run: {new Date(workflow.last_executed).toLocaleString()}</span>
                        )}
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      <Switch
                        checked={workflow.is_active}
                        onCheckedChange={(checked) => toggleWorkflow(workflow.id, checked)}
                      />
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() => executeWorkflow(workflow.id)}
                        disabled={!workflow.is_active}
                      >
                        <Play className="w-3 h-3 mr-1" />
                        Run
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() => deleteWorkflow(workflow.id, workflow.name)}
                        className="text-red-600 hover:text-red-700 hover:bg-red-50"
                      >
                        <Trash2 className="w-3 h-3" />
                      </Button>
                    </div>
                  </div>

                  {workflow.actions && workflow.actions.length > 0 && (
                    <div className="bg-gray-50 p-3 rounded">
                      <div className="text-sm font-medium mb-2">Workflow Actions:</div>
                      <div className="text-xs space-y-1">
                        {workflow.actions.map((action, index) => (
                          <div key={index} className="flex items-center gap-2">
                            <span className="w-4 h-4 rounded-full bg-blue-100 text-blue-600 text-xs flex items-center justify-center">
                              {action.order || index + 1}
                            </span>
                            <span>{action.type}</span>
                            <Badge variant="outline" className="text-xs">{action.type}</Badge>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Recent Workflow Runs */}
      {workflowRuns.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Clock className="w-5 h-5" />
              Recent Workflow Runs
            </CardTitle>
            <CardDescription>
              Track the execution status of your workflows
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {workflowRuns.slice(0, 10).map((run) => (
                <div key={run.id} className="flex items-center justify-between p-3 border rounded-lg">
                  <div className="flex items-center gap-3">
                    {getStatusIcon(run.status)}
                    <div>
                      <div className="font-medium text-sm">{run.workflow_name}</div>
                      <div className="text-xs text-gray-500">
                        Started: {new Date(run.started_at).toLocaleString()}
                        {run.completed_at && (
                          <span> | Completed: {new Date(run.completed_at).toLocaleString()}</span>
                        )}
                      </div>
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    <Badge variant={getStatusColor(run.status)}>
                      {run.status}
                    </Badge>
                    {run.progress !== undefined && (
                      <div className="w-16">
                        <Progress value={run.progress} className="h-2" />
                      </div>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )
}

export default WorkflowAutomation
