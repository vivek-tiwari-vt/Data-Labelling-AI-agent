import React, { useState, useEffect } from 'react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card.jsx'
import { Button } from '@/components/ui/button.jsx'
import { Input } from '@/components/ui/input.jsx'
import { Textarea } from '@/components/ui/textarea.jsx'
import { Badge } from '@/components/ui/badge.jsx'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select.jsx'
import { 
  Plus, 
  Search, 
  Star, 
  Edit3, 
  Trash2, 
  Copy, 
  Tag,
  FileText,
  Filter,
  TrendingUp
} from 'lucide-react'

const LabelTemplateManager = ({ onTemplateSelect }) => {
  const [templates, setTemplates] = useState([])
  const [domains, setDomains] = useState([])
  const [loading, setLoading] = useState(true)
  const [searchTerm, setSearchTerm] = useState('')
  const [selectedDomain, setSelectedDomain] = useState('all')
  const [showCreateForm, setShowCreateForm] = useState(false)
  const [editingTemplate, setEditingTemplate] = useState(null)
  const [error, setError] = useState(null)

  // New template form data
  const [newTemplate, setNewTemplate] = useState({
    name: '',
    description: '',
    labels: '',
    instructions: '',
    domain: 'general',
    is_public: false
  })

  useEffect(() => {
    fetchTemplates()
    fetchDomains()
  }, [])

  const fetchTemplates = async () => {
    try {
      const response = await fetch('http://localhost:8000/api/v1/templates/')
      if (!response.ok) throw new Error('Failed to fetch templates')
      
      const data = await response.json()
      setTemplates(data.templates || [])
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  const fetchDomains = async () => {
    try {
      const response = await fetch('http://localhost:8000/api/v1/templates/domains/list')
      if (!response.ok) throw new Error('Failed to fetch domains')
      
      const data = await response.json()
      setDomains(data.domains || [])
    } catch (err) {
      console.error('Failed to fetch domains:', err)
    }
  }

  const createTemplate = async () => {
    try {
      const labelsArray = newTemplate.labels.split(',').map(l => l.trim()).filter(l => l)
      
      const response = await fetch('http://localhost:8000/api/v1/templates/', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ...newTemplate,
          labels: labelsArray
        })
      })

      if (!response.ok) throw new Error('Failed to create template')

      await fetchTemplates()
      setShowCreateForm(false)
      setNewTemplate({
        name: '',
        description: '',
        labels: '',
        instructions: '',
        domain: 'general',
        is_public: false
      })
    } catch (err) {
      setError(err.message)
    }
  }

  const deleteTemplate = async (templateId) => {
    if (!confirm('Are you sure you want to delete this template?')) return

    try {
      const response = await fetch(`http://localhost:8000/api/v1/templates/${templateId}`, {
        method: 'DELETE'
      })

      if (!response.ok) throw new Error('Failed to delete template')

      await fetchTemplates()
    } catch (err) {
      setError(err.message)
    }
  }

  const useTemplate = async (template) => {
    try {
      // Record usage
      await fetch(`http://localhost:8000/api/v1/templates/${template.id}/use`, {
        method: 'POST'
      })

      // Notify parent component
      if (onTemplateSelect) {
        onTemplateSelect({
          labels: template.labels.join(', '),
          instructions: template.instructions
        })
      }

      await fetchTemplates() // Refresh to update usage count
    } catch (err) {
      console.error('Failed to record template usage:', err)
      // Still proceed with template selection
      if (onTemplateSelect) {
        onTemplateSelect({
          labels: template.labels.join(', '),
          instructions: template.instructions
        })
      }
    }
  }

  const filteredTemplates = templates.filter(template => {
    const matchesSearch = template.name.toLowerCase().includes(searchTerm.toLowerCase()) ||
                         template.description.toLowerCase().includes(searchTerm.toLowerCase()) ||
                         template.labels.some(label => label.toLowerCase().includes(searchTerm.toLowerCase()))
    
    const matchesDomain = selectedDomain === 'all' || template.domain === selectedDomain

    return matchesSearch && matchesDomain
  })

  if (loading) {
    return (
      <Card>
        <CardContent className="pt-6">
          <div className="text-center">Loading templates...</div>
        </CardContent>
      </Card>
    )
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold">Label Templates</h2>
          <p className="text-gray-600">Save and reuse label configurations</p>
        </div>
        <Button onClick={() => setShowCreateForm(!showCreateForm)}>
          <Plus className="h-4 w-4 mr-2" />
          New Template
        </Button>
      </div>

      {/* Error display */}
      {error && (
        <Card className="border-red-200 bg-red-50">
          <CardContent className="pt-6">
            <p className="text-red-600">{error}</p>
            <Button 
              variant="outline" 
              onClick={() => setError(null)}
              className="mt-2"
            >
              Dismiss
            </Button>
          </CardContent>
        </Card>
      )}

      {/* Create Template Form */}
      {showCreateForm && (
        <Card>
          <CardHeader>
            <CardTitle>Create New Template</CardTitle>
            <CardDescription>Save a reusable label configuration</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium mb-2">Template Name</label>
                <Input
                  value={newTemplate.name}
                  onChange={(e) => setNewTemplate({...newTemplate, name: e.target.value})}
                  placeholder="e.g., Product Reviews"
                />
              </div>
              <div>
                <label className="block text-sm font-medium mb-2">Domain</label>
                <Select 
                  value={newTemplate.domain}
                  onValueChange={(value) => setNewTemplate({...newTemplate, domain: value})}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="general">General</SelectItem>
                    <SelectItem value="e-commerce">E-commerce</SelectItem>
                    <SelectItem value="customer_service">Customer Service</SelectItem>
                    <SelectItem value="content_safety">Content Safety</SelectItem>
                    <SelectItem value="journalism">Journalism</SelectItem>
                    <SelectItem value="social_media">Social Media</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>

            <div>
              <label className="block text-sm font-medium mb-2">Description</label>
              <Input
                value={newTemplate.description}
                onChange={(e) => setNewTemplate({...newTemplate, description: e.target.value})}
                placeholder="Brief description of when to use this template"
              />
            </div>

            <div>
              <label className="block text-sm font-medium mb-2">Labels</label>
              <Input
                value={newTemplate.labels}
                onChange={(e) => setNewTemplate({...newTemplate, labels: e.target.value})}
                placeholder="positive, negative, neutral, question, complaint"
              />
              <p className="text-xs text-gray-500 mt-1">Separate labels with commas</p>
            </div>

            <div>
              <label className="block text-sm font-medium mb-2">Instructions</label>
              <Textarea
                value={newTemplate.instructions}
                onChange={(e) => setNewTemplate({...newTemplate, instructions: e.target.value})}
                placeholder="Instructions for how to classify using these labels"
                rows={3}
              />
            </div>

            <div className="flex items-center space-x-2">
              <input
                type="checkbox"
                id="is_public"
                checked={newTemplate.is_public}
                onChange={(e) => setNewTemplate({...newTemplate, is_public: e.target.checked})}
              />
              <label htmlFor="is_public" className="text-sm">Make this template public</label>
            </div>

            <div className="flex gap-2">
              <Button onClick={createTemplate}>Create Template</Button>
              <Button variant="outline" onClick={() => setShowCreateForm(false)}>Cancel</Button>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Search and Filter */}
      <Card>
        <CardContent className="pt-6">
          <div className="flex gap-4">
            <div className="flex-1">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-gray-400" />
                <Input
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  placeholder="Search templates..."
                  className="pl-10"
                />
              </div>
            </div>
            <Select value={selectedDomain} onValueChange={setSelectedDomain}>
              <SelectTrigger className="w-48">
                <SelectValue placeholder="Filter by domain" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">All Domains</SelectItem>
                {domains.map(domain => (
                  <SelectItem key={domain} value={domain}>{domain}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </CardContent>
      </Card>

      {/* Templates Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {filteredTemplates.map(template => (
          <Card key={template.id} className="hover:shadow-md transition-shadow">
            <CardHeader>
              <div className="flex items-start justify-between">
                <div>
                  <CardTitle className="text-lg">{template.name}</CardTitle>
                  <CardDescription>{template.description}</CardDescription>
                </div>
                <div className="flex items-center space-x-1">
                  <Badge variant="outline">{template.domain}</Badge>
                  {template.is_public && <Star className="h-4 w-4 text-yellow-500" />}
                </div>
              </div>
            </CardHeader>
            <CardContent>
              <div className="space-y-3">
                {/* Labels */}
                <div>
                  <p className="text-sm font-medium mb-2">Labels ({template.labels.length})</p>
                  <div className="flex flex-wrap gap-1">
                    {template.labels.slice(0, 4).map(label => (
                      <Badge key={label} variant="secondary" className="text-xs">
                        {label}
                      </Badge>
                    ))}
                    {template.labels.length > 4 && (
                      <Badge variant="outline" className="text-xs">
                        +{template.labels.length - 4} more
                      </Badge>
                    )}
                  </div>
                </div>

                {/* Instructions Preview */}
                <div>
                  <p className="text-sm font-medium mb-1">Instructions</p>
                  <p className="text-xs text-gray-600 line-clamp-2">
                    {template.instructions.slice(0, 100)}
                    {template.instructions.length > 100 && '...'}
                  </p>
                </div>

                {/* Usage Stats */}
                <div className="flex items-center justify-between text-xs text-gray-500">
                  <span>Used {template.usage_count} times</span>
                  <span>{new Date(template.created_at).toLocaleDateString()}</span>
                </div>

                {/* Actions */}
                <div className="flex gap-2 pt-2">
                  <Button 
                    size="sm" 
                    onClick={() => useTemplate(template)}
                    className="flex-1"
                  >
                    <Tag className="h-3 w-3 mr-1" />
                    Use Template
                  </Button>
                  <Button 
                    size="sm" 
                    variant="outline"
                    onClick={() => setEditingTemplate(template)}
                  >
                    <Edit3 className="h-3 w-3" />
                  </Button>
                  {template.created_by !== 'system' && (
                    <Button 
                      size="sm" 
                      variant="outline"
                      onClick={() => deleteTemplate(template.id)}
                    >
                      <Trash2 className="h-3 w-3" />
                    </Button>
                  )}
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      {filteredTemplates.length === 0 && (
        <Card>
          <CardContent className="pt-6">
            <div className="text-center text-gray-500">
              <FileText className="h-8 w-8 mx-auto mb-2" />
              <p>No templates found matching your criteria</p>
              <Button 
                variant="outline" 
                onClick={() => setShowCreateForm(true)}
                className="mt-2"
              >
                Create your first template
              </Button>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )
}

export default LabelTemplateManager
