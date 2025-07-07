import { useState, useEffect } from 'react'
import { Button } from '@/components/ui/button.jsx'
import { Input } from '@/components/ui/input.jsx'
import { Textarea } from '@/components/ui/textarea.jsx'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select.jsx'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card.jsx'
import { Badge } from '@/components/ui/badge.jsx'
import { Switch } from '@/components/ui/switch.jsx'
import { Shield, CheckCircle, XCircle, AlertTriangle, Plus, Trash2, Play, Settings } from 'lucide-react'

const AdvancedValidation = () => {
  const [validationRules, setValidationRules] = useState([])
  const [newRule, setNewRule] = useState({
    name: '',
    type: 'format',
    condition: '',
    expected_value: '',
    error_message: '',
    severity: 'error',
    enabled: true
  })
  const [jobId, setJobId] = useState('')
  const [validationResults, setValidationResults] = useState(null)
  const [isValidating, setIsValidating] = useState(false)

  const ruleTypes = [
    { value: 'format', label: 'Format Validation', description: 'Validate data format and structure' },
    { value: 'range', label: 'Range Validation', description: 'Validate numeric ranges and bounds' },
    { value: 'pattern', label: 'Pattern Validation', description: 'Validate using regex patterns' },
    { value: 'length', label: 'Length Validation', description: 'Validate text length constraints' },
    { value: 'required', label: 'Required Field', description: 'Ensure required fields are present' },
    { value: 'unique', label: 'Uniqueness Check', description: 'Validate field uniqueness' },
    { value: 'custom', label: 'Custom Logic', description: 'Custom validation logic' }
  ]

  const severityLevels = [
    { value: 'error', label: 'Error', color: 'destructive' },
    { value: 'warning', label: 'Warning', color: 'warning' },
    { value: 'info', label: 'Info', color: 'secondary' }
  ]

  useEffect(() => {
    fetchValidationRules()
  }, [])

  const fetchValidationRules = async () => {
    try {
      const response = await fetch('http://localhost:8000/api/v1/validation/rules')
      if (response.ok) {
        const data = await response.json()
        setValidationRules(data.rules || [])
      }
    } catch (error) {
      console.error('Error fetching validation rules:', error)
    }
  }

  const createValidationRule = async () => {
    if (!newRule.name.trim() || !newRule.condition.trim()) {
      alert('Please provide rule name and condition')
      return
    }

    try {
      const response = await fetch('http://localhost:8000/api/v1/validation/rules', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(newRule)
      })

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`)
      }

      const result = await response.json()
      alert('Validation rule created successfully!')
      
      // Reset form
      setNewRule({
        name: '',
        type: 'format',
        condition: '',
        expected_value: '',
        error_message: '',
        severity: 'error',
        enabled: true
      })
      
      // Refresh rules
      fetchValidationRules()
    } catch (error) {
      console.error('Error creating validation rule:', error)
      alert('Error creating validation rule: ' + error.message)
    }
  }

  const toggleRuleStatus = async (ruleId, enabled) => {
    try {
      const response = await fetch(`http://localhost:8000/api/v1/validation/rules/${ruleId}`, {
        method: 'PATCH',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ enabled })
      })

      if (response.ok) {
        setValidationRules(prev => prev.map(rule => 
          rule.id === ruleId ? { ...rule, enabled } : rule
        ))
      }
    } catch (error) {
      console.error('Error updating rule status:', error)
    }
  }

  const deleteValidationRule = async (ruleId) => {
    if (!confirm('Are you sure you want to delete this validation rule?')) {
      return
    }

    try {
      const response = await fetch(`http://localhost:8000/api/v1/validation/rules/${ruleId}`, {
        method: 'DELETE'
      })

      if (response.ok) {
        setValidationRules(prev => prev.filter(rule => rule.id !== ruleId))
        alert('Validation rule deleted successfully!')
      }
    } catch (error) {
      console.error('Error deleting validation rule:', error)
      alert('Error deleting validation rule: ' + error.message)
    }
  }

  const validateJob = async () => {
    if (!jobId.trim()) {
      alert('Please enter a job ID')
      return
    }

    setIsValidating(true)
    try {
      const response = await fetch(`http://localhost:8000/api/v1/validation/validate/${jobId}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        }
      })

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`)
      }

      const data = await response.json()
      setValidationResults(data)
    } catch (error) {
      console.error('Error validating job:', error)
      alert('Error validating job: ' + error.message)
    } finally {
      setIsValidating(false)
    }
  }

  const getValidationReport = async (jobId) => {
    try {
      const response = await fetch(`http://localhost:8000/api/v1/validation/report/${jobId}`)
      if (response.ok) {
        const data = await response.json()
        alert('Validation Report:\n' + JSON.stringify(data, null, 2))
      }
    } catch (error) {
      console.error('Error fetching validation report:', error)
      alert('Error fetching validation report: ' + error.message)
    }
  }

  return (
    <div className="space-y-6">
      <div className="text-center py-6">
        <Shield className="w-12 h-12 mx-auto text-purple-500 mb-4" />
        <h2 className="text-2xl font-bold text-gray-900 mb-2">Advanced Validation</h2>
        <p className="text-gray-600 max-w-2xl mx-auto">
          Define custom validation rules and ensure data quality across your labeling pipeline
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Create Validation Rule */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Plus className="w-5 h-5" />
              Create Validation Rule
            </CardTitle>
            <CardDescription>
              Define custom validation logic for your data quality requirements
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <label className="block text-sm font-medium mb-2">Rule Name</label>
              <Input
                value={newRule.name}
                onChange={(e) => setNewRule(prev => ({ ...prev, name: e.target.value }))}
                placeholder="Enter descriptive rule name"
              />
            </div>

            <div>
              <label className="block text-sm font-medium mb-2">Validation Type</label>
              <Select 
                value={newRule.type} 
                onValueChange={(value) => setNewRule(prev => ({ ...prev, type: value }))}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Select validation type" />
                </SelectTrigger>
                <SelectContent>
                  {ruleTypes.map((type) => (
                    <SelectItem key={type.value} value={type.value}>
                      <div>
                        <div className="font-medium">{type.label}</div>
                        <div className="text-xs text-gray-500">{type.description}</div>
                      </div>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div>
              <label className="block text-sm font-medium mb-2">Condition</label>
              <Textarea
                value={newRule.condition}
                onChange={(e) => setNewRule(prev => ({ ...prev, condition: e.target.value }))}
                placeholder="Define validation condition (e.g., length > 10, matches regex, etc.)"
                rows={3}
              />
            </div>

            <div>
              <label className="block text-sm font-medium mb-2">Expected Value (optional)</label>
              <Input
                value={newRule.expected_value}
                onChange={(e) => setNewRule(prev => ({ ...prev, expected_value: e.target.value }))}
                placeholder="Expected value or pattern"
              />
            </div>

            <div>
              <label className="block text-sm font-medium mb-2">Error Message</label>
              <Textarea
                value={newRule.error_message}
                onChange={(e) => setNewRule(prev => ({ ...prev, error_message: e.target.value }))}
                placeholder="Custom error message for validation failures"
                rows={2}
              />
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium mb-2">Severity</label>
                <Select 
                  value={newRule.severity} 
                  onValueChange={(value) => setNewRule(prev => ({ ...prev, severity: value }))}
                >
                  <SelectTrigger>
                    <SelectValue placeholder="Select severity" />
                  </SelectTrigger>
                  <SelectContent>
                    {severityLevels.map((level) => (
                      <SelectItem key={level.value} value={level.value}>
                        <Badge variant={level.color}>{level.label}</Badge>
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div className="flex items-center space-x-2 pt-6">
                <Switch
                  checked={newRule.enabled}
                  onCheckedChange={(checked) => setNewRule(prev => ({ ...prev, enabled: checked }))}
                />
                <label className="text-sm font-medium">Enabled</label>
              </div>
            </div>

            <Button onClick={createValidationRule} className="w-full">
              Create Validation Rule
            </Button>
          </CardContent>
        </Card>

        {/* Job Validation */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Play className="w-5 h-5" />
              Job Validation
            </CardTitle>
            <CardDescription>
              Validate a specific job against defined rules
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <label className="block text-sm font-medium mb-2">Job ID</label>
              <Input
                value={jobId}
                onChange={(e) => setJobId(e.target.value)}
                placeholder="Enter job ID to validate"
              />
            </div>

            <div className="flex gap-2">
              <Button 
                onClick={validateJob} 
                disabled={isValidating || !jobId.trim()}
                className="flex-1"
              >
                {isValidating ? 'Validating...' : 'Validate Job'}
              </Button>
              <Button 
                variant="outline" 
                onClick={() => getValidationReport(jobId)}
                disabled={!jobId.trim()}
              >
                Get Report
              </Button>
            </div>

            {validationResults && (
              <div className="mt-4 space-y-3">
                <div className="grid grid-cols-3 gap-4 text-center">
                  <div className="p-3 bg-green-50 rounded-lg">
                    <div className="text-lg font-bold text-green-600">
                      {validationResults.passed || 0}
                    </div>
                    <div className="text-xs text-gray-600">Passed</div>
                  </div>
                  <div className="p-3 bg-yellow-50 rounded-lg">
                    <div className="text-lg font-bold text-yellow-600">
                      {validationResults.warnings || 0}
                    </div>
                    <div className="text-xs text-gray-600">Warnings</div>
                  </div>
                  <div className="p-3 bg-red-50 rounded-lg">
                    <div className="text-lg font-bold text-red-600">
                      {validationResults.errors || 0}
                    </div>
                    <div className="text-xs text-gray-600">Errors</div>
                  </div>
                </div>

                {validationResults.issues && validationResults.issues.length > 0 && (
                  <div className="space-y-2">
                    <h4 className="font-medium">Issues Found:</h4>
                    {validationResults.issues.slice(0, 5).map((issue, index) => (
                      <div key={index} className="flex items-start gap-2 p-2 bg-gray-50 rounded text-sm">
                        {issue.severity === 'error' ? (
                          <XCircle className="w-4 h-4 text-red-500 mt-0.5" />
                        ) : issue.severity === 'warning' ? (
                          <AlertTriangle className="w-4 h-4 text-yellow-500 mt-0.5" />
                        ) : (
                          <CheckCircle className="w-4 h-4 text-blue-500 mt-0.5" />
                        )}
                        <div>
                          <div className="font-medium">{issue.rule_name}</div>
                          <div className="text-gray-600">{issue.message}</div>
                        </div>
                      </div>
                    ))}
                    {validationResults.issues.length > 5 && (
                      <div className="text-sm text-gray-500 text-center">
                        ... and {validationResults.issues.length - 5} more issues
                      </div>
                    )}
                  </div>
                )}
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Validation Rules List */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Settings className="w-5 h-5" />
            Validation Rules
          </CardTitle>
          <CardDescription>
            Manage your validation rules and their status
          </CardDescription>
        </CardHeader>
        <CardContent>
          {validationRules.length === 0 ? (
            <div className="text-center py-8 text-gray-500">
              <Shield className="w-8 h-8 mx-auto mb-2 opacity-50" />
              <p>No validation rules defined. Create your first rule above.</p>
            </div>
          ) : (
            <div className="space-y-4">
              {validationRules.map((rule) => (
                <div key={rule.id} className="border rounded-lg p-4">
                  <div className="flex justify-between items-start mb-2">
                    <div className="flex-1">
                      <div className="flex items-center gap-2 mb-1">
                        <h4 className="font-medium">{rule.name}</h4>
                        <Badge variant={rule.enabled ? 'default' : 'secondary'}>
                          {rule.enabled ? 'Enabled' : 'Disabled'}
                        </Badge>
                        <Badge variant={
                          rule.severity === 'error' ? 'destructive' :
                          rule.severity === 'warning' ? 'warning' :
                          'secondary'
                        }>
                          {rule.severity}
                        </Badge>
                      </div>
                      <div className="text-sm text-gray-600 mb-2">
                        Type: {ruleTypes.find(t => t.value === rule.type)?.label || rule.type}
                      </div>
                      <div className="text-sm bg-gray-50 p-2 rounded font-mono">
                        {rule.condition}
                      </div>
                      {rule.error_message && (
                        <div className="text-sm text-gray-600 mt-2">
                          Error message: {rule.error_message}
                        </div>
                      )}
                    </div>
                    <div className="flex items-center gap-2 ml-4">
                      <Switch
                        checked={rule.enabled}
                        onCheckedChange={(checked) => toggleRuleStatus(rule.id, checked)}
                      />
                      <Button
                        size="sm"
                        variant="destructive"
                        onClick={() => deleteValidationRule(rule.id)}
                      >
                        <Trash2 className="w-3 h-3" />
                      </Button>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  )
}

export default AdvancedValidation
