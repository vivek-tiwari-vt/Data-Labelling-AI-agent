import { useState, useEffect } from 'react'
import { Button } from '@/components/ui/button.jsx'
import { Input } from '@/components/ui/input.jsx'
import { Textarea } from '@/components/ui/textarea.jsx'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select.jsx'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card.jsx'
import { Badge } from '@/components/ui/badge.jsx'
import { Switch } from '@/components/ui/switch.jsx'
import { Zap, Plus, Settings, CheckCircle, XCircle, ExternalLink, Database, Cloud, Webhook } from 'lucide-react'

const IntegrationHub = () => {
  const [integrations, setIntegrations] = useState([])
  const [availableServices, setAvailableServices] = useState([])
  const [newIntegration, setNewIntegration] = useState({
    name: '',
    service_type: '',
    endpoint_url: '',
    api_key: '',
    config: '',
    enabled: true
  })
  const [testResults, setTestResults] = useState({})
  const [isCreating, setIsCreating] = useState(false)

  const serviceTypes = [
    { 
      value: 'webhook', 
      label: 'Webhook', 
      description: 'HTTP webhook for real-time notifications',
      icon: Webhook
    },
    { 
      value: 'database', 
      label: 'Database', 
      description: 'Database connection for data export',
      icon: Database
    },
    { 
      value: 'cloud_storage', 
      label: 'Cloud Storage', 
      description: 'Cloud storage service integration',
      icon: Cloud
    },
    { 
      value: 'api', 
      label: 'REST API', 
      description: 'REST API integration for data exchange',
      icon: ExternalLink
    }
  ]

  useEffect(() => {
    fetchIntegrations()
    fetchAvailableServices()
  }, [])

  const fetchIntegrations = async () => {
    try {
      const response = await fetch('http://localhost:8000/api/v1/integration/integrations')
      if (response.ok) {
        const data = await response.json()
        setIntegrations(data.integrations || [])
      }
    } catch (error) {
      console.error('Error fetching integrations:', error)
    }
  }

  const fetchAvailableServices = async () => {
    try {
      const response = await fetch('http://localhost:8000/api/v1/integration/services')
      if (response.ok) {
        const data = await response.json()
        setAvailableServices(data.services || [])
      }
    } catch (error) {
      console.error('Error fetching available services:', error)
    }
  }

  const createIntegration = async () => {
    if (!newIntegration.name.trim() || !newIntegration.service_type || !newIntegration.endpoint_url.trim()) {
      alert('Please provide integration name, service type, and endpoint URL')
      return
    }

    setIsCreating(true)
    try {
      let config = {}
      if (newIntegration.config.trim()) {
        try {
          config = JSON.parse(newIntegration.config)
        } catch {
          alert('Invalid JSON in configuration field')
          setIsCreating(false)
          return
        }
      }

      const response = await fetch('http://localhost:8000/api/v1/integration/integrations', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          ...newIntegration,
          config
        })
      })

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`)
      }

      const result = await response.json()
      alert('Integration created successfully!')
      
      // Reset form
      setNewIntegration({
        name: '',
        service_type: '',
        endpoint_url: '',
        api_key: '',
        config: '',
        enabled: true
      })
      
      // Refresh integrations
      fetchIntegrations()
    } catch (error) {
      console.error('Error creating integration:', error)
      alert('Error creating integration: ' + error.message)
    } finally {
      setIsCreating(false)
    }
  }

  const testIntegration = async (integrationId) => {
    setTestResults(prev => ({ ...prev, [integrationId]: { testing: true } }))
    
    try {
      const response = await fetch(`http://localhost:8000/api/v1/integration/test/${integrationId}`, {
        method: 'POST'
      })

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`)
      }

      const data = await response.json()
      setTestResults(prev => ({ 
        ...prev, 
        [integrationId]: { 
          testing: false, 
          success: data.success,
          message: data.message,
          response_time: data.response_time
        } 
      }))
    } catch (error) {
      console.error('Error testing integration:', error)
      setTestResults(prev => ({ 
        ...prev, 
        [integrationId]: { 
          testing: false, 
          success: false,
          message: error.message
        } 
      }))
    }
  }

  const toggleIntegration = async (integrationId, enabled) => {
    try {
      const response = await fetch(`http://localhost:8000/api/v1/integration/integrations/${integrationId}`, {
        method: 'PATCH',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ enabled })
      })

      if (response.ok) {
        setIntegrations(prev => prev.map(integration => 
          integration.id === integrationId ? { ...integration, enabled } : integration
        ))
      }
    } catch (error) {
      console.error('Error toggling integration:', error)
    }
  }

  const triggerIntegration = async (integrationId, data = {}) => {
    try {
      const response = await fetch(`http://localhost:8000/api/v1/integration/trigger/${integrationId}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(data)
      })

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`)
      }

      const result = await response.json()
      alert('Integration triggered successfully!\nResponse: ' + JSON.stringify(result, null, 2))
    } catch (error) {
      console.error('Error triggering integration:', error)
      alert('Error triggering integration: ' + error.message)
    }
  }

  const getServiceIcon = (serviceType) => {
    const service = serviceTypes.find(s => s.value === serviceType)
    const IconComponent = service?.icon || ExternalLink
    return <IconComponent className="w-4 h-4" />
  }

  return (
    <div className="space-y-6">
      <div className="text-center py-6">
        <Zap className="w-12 h-12 mx-auto text-orange-500 mb-4" />
        <h2 className="text-2xl font-bold text-gray-900 mb-2">Integration Hub</h2>
        <p className="text-gray-600 max-w-2xl mx-auto">
          Connect with external services and automate data flow across your labeling pipeline
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Create Integration */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Plus className="w-5 h-5" />
              Create Integration
            </CardTitle>
            <CardDescription>
              Set up a new integration with external services
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <label className="block text-sm font-medium mb-2">Integration Name</label>
              <Input
                value={newIntegration.name}
                onChange={(e) => setNewIntegration(prev => ({ ...prev, name: e.target.value }))}
                placeholder="Enter descriptive integration name"
              />
            </div>

            <div>
              <label className="block text-sm font-medium mb-2">Service Type</label>
              <Select 
                value={newIntegration.service_type} 
                onValueChange={(value) => setNewIntegration(prev => ({ ...prev, service_type: value }))}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Select service type" />
                </SelectTrigger>
                <SelectContent>
                  {serviceTypes.map((service) => (
                    <SelectItem key={service.value} value={service.value}>
                      <div className="flex items-center gap-2">
                        <service.icon className="w-4 h-4" />
                        <div>
                          <div className="font-medium">{service.label}</div>
                          <div className="text-xs text-gray-500">{service.description}</div>
                        </div>
                      </div>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div>
              <label className="block text-sm font-medium mb-2">Endpoint URL</label>
              <Input
                value={newIntegration.endpoint_url}
                onChange={(e) => setNewIntegration(prev => ({ ...prev, endpoint_url: e.target.value }))}
                placeholder="https://api.example.com/webhook"
              />
            </div>

            <div>
              <label className="block text-sm font-medium mb-2">API Key (optional)</label>
              <Input
                type="password"
                value={newIntegration.api_key}
                onChange={(e) => setNewIntegration(prev => ({ ...prev, api_key: e.target.value }))}
                placeholder="Enter API key for authentication"
              />
            </div>

            <div>
              <label className="block text-sm font-medium mb-2">Configuration (JSON)</label>
              <Textarea
                value={newIntegration.config}
                onChange={(e) => setNewIntegration(prev => ({ ...prev, config: e.target.value }))}
                placeholder='{"timeout": 30, "retries": 3, "headers": {}}'
                rows={4}
                className="font-mono text-sm"
              />
            </div>

            <div className="flex items-center space-x-2">
              <Switch
                checked={newIntegration.enabled}
                onCheckedChange={(checked) => setNewIntegration(prev => ({ ...prev, enabled: checked }))}
              />
              <label className="text-sm font-medium">Enable integration</label>
            </div>

            <Button onClick={createIntegration} disabled={isCreating} className="w-full">
              {isCreating ? 'Creating...' : 'Create Integration'}
            </Button>
          </CardContent>
        </Card>

        {/* Available Services */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Settings className="w-5 h-5" />
              Available Services
            </CardTitle>
            <CardDescription>
              Supported external services and their capabilities
            </CardDescription>
          </CardHeader>
          <CardContent>
            {availableServices.length === 0 ? (
              <div className="grid grid-cols-1 gap-3">
                {serviceTypes.map((service) => (
                  <div key={service.value} className="flex items-center gap-3 p-3 border rounded-lg">
                    <service.icon className="w-6 h-6 text-gray-400" />
                    <div>
                      <div className="font-medium">{service.label}</div>
                      <div className="text-sm text-gray-600">{service.description}</div>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <div className="space-y-3">
                {availableServices.map((service) => (
                  <div key={service.id} className="flex items-center gap-3 p-3 border rounded-lg">
                    {getServiceIcon(service.type)}
                    <div className="flex-1">
                      <div className="font-medium">{service.name}</div>
                      <div className="text-sm text-gray-600">{service.description}</div>
                    </div>
                    <Badge variant={service.status === 'active' ? 'default' : 'secondary'}>
                      {service.status}
                    </Badge>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Integrations List */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Zap className="w-5 h-5" />
            Active Integrations
          </CardTitle>
          <CardDescription>
            Manage your configured integrations
          </CardDescription>
        </CardHeader>
        <CardContent>
          {integrations.length === 0 ? (
            <div className="text-center py-8 text-gray-500">
              <Zap className="w-8 h-8 mx-auto mb-2 opacity-50" />
              <p>No integrations configured. Create your first integration above.</p>
            </div>
          ) : (
            <div className="space-y-4">
              {integrations.map((integration) => (
                <div key={integration.id} className="border rounded-lg p-4">
                  <div className="flex justify-between items-start mb-3">
                    <div className="flex-1">
                      <div className="flex items-center gap-2 mb-1">
                        {getServiceIcon(integration.service_type)}
                        <h4 className="font-medium">{integration.name}</h4>
                        <Badge variant={integration.enabled ? 'default' : 'secondary'}>
                          {integration.enabled ? 'Enabled' : 'Disabled'}
                        </Badge>
                        <Badge variant="outline">
                          {serviceTypes.find(s => s.value === integration.service_type)?.label || integration.service_type}
                        </Badge>
                      </div>
                      <div className="text-sm text-gray-600 mb-2">
                        Endpoint: {integration.endpoint_url}
                      </div>
                      {integration.last_triggered && (
                        <div className="text-xs text-gray-500">
                          Last triggered: {new Date(integration.last_triggered).toLocaleString()}
                        </div>
                      )}
                    </div>
                    <div className="flex items-center gap-2">
                      <Switch
                        checked={integration.enabled}
                        onCheckedChange={(checked) => toggleIntegration(integration.id, checked)}
                      />
                    </div>
                  </div>

                  <div className="flex gap-2">
                    <Button
                      size="sm"
                      variant="outline"
                      onClick={() => testIntegration(integration.id)}
                      disabled={testResults[integration.id]?.testing}
                    >
                      {testResults[integration.id]?.testing ? 'Testing...' : 'Test'}
                    </Button>
                    <Button
                      size="sm"
                      variant="outline"
                      onClick={() => triggerIntegration(integration.id, { test: true })}
                      disabled={!integration.enabled}
                    >
                      Trigger
                    </Button>
                  </div>

                  {testResults[integration.id] && !testResults[integration.id].testing && (
                    <div className={`mt-3 p-2 rounded text-sm flex items-center gap-2 ${
                      testResults[integration.id].success 
                        ? 'bg-green-50 text-green-700' 
                        : 'bg-red-50 text-red-700'
                    }`}>
                      {testResults[integration.id].success ? (
                        <CheckCircle className="w-4 h-4" />
                      ) : (
                        <XCircle className="w-4 h-4" />
                      )}
                      <div>
                        <div>{testResults[integration.id].message}</div>
                        {testResults[integration.id].response_time && (
                          <div className="text-xs opacity-75">
                            Response time: {testResults[integration.id].response_time}ms
                          </div>
                        )}
                      </div>
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

export default IntegrationHub
