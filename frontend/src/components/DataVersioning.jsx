import { useState, useEffect } from 'react'
import { Button } from '@/components/ui/button.jsx'
import { Input } from '@/components/ui/input.jsx'
import { Textarea } from '@/components/ui/textarea.jsx'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select.jsx'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card.jsx'
import { Badge } from '@/components/ui/badge.jsx'
import { GitBranch, Clock, User, FileText, Download, Upload, Eye, Tag } from 'lucide-react'

const DataVersioning = () => {
  const [entities, setEntities] = useState([])
  const [selectedEntity, setSelectedEntity] = useState('')
  const [versions, setVersions] = useState([])
  const [newEntityId, setNewEntityId] = useState('')
  const [changeType, setChangeType] = useState('create')
  const [changeDescription, setChangeDescription] = useState('')
  const [versionData, setVersionData] = useState('')
  const [isLoading, setIsLoading] = useState(false)

  const changeTypes = [
    { value: 'create', label: 'Create', description: 'Initial creation of entity' },
    { value: 'update', label: 'Update', description: 'Modification of existing entity' },
    { value: 'delete', label: 'Delete', description: 'Deletion of entity' },
    { value: 'merge', label: 'Merge', description: 'Merging with another entity' },
    { value: 'split', label: 'Split', description: 'Splitting into multiple entities' }
  ]

  useEffect(() => {
    fetchEntities()
  }, [])

  const fetchEntities = async () => {
    try {
      const response = await fetch('http://localhost:8000/api/v1/versioning/entities')
      if (response.ok) {
        const data = await response.json()
        setEntities(data.entities || [])
      }
    } catch (error) {
      console.error('Error fetching entities:', error)
    }
  }

  const fetchVersions = async (entityId) => {
    setIsLoading(true)
    try {
      const response = await fetch(`http://localhost:8000/api/v1/versioning/entities/${entityId}/versions`)
      if (response.ok) {
        const data = await response.json()
        setVersions(data.versions || [])
      }
    } catch (error) {
      console.error('Error fetching versions:', error)
    } finally {
      setIsLoading(false)
    }
  }

  const createNewVersion = async () => {
    if (!newEntityId.trim() || !versionData.trim()) {
      alert('Please provide entity ID and version data')
      return
    }

    try {
      let parsedData
      try {
        parsedData = JSON.parse(versionData)
      } catch {
        // If not JSON, treat as plain text
        parsedData = { content: versionData }
      }

      const response = await fetch('http://localhost:8000/api/v1/versioning/entities', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          entity_id: newEntityId,
          data: parsedData,
          change_type: changeType,
          description: changeDescription
        })
      })

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`)
      }

      const result = await response.json()
      alert('Version created successfully!')
      
      // Reset form
      setVersionData('')
      setChangeDescription('')
      
      // Refresh data
      fetchEntities()
      if (selectedEntity === newEntityId) {
        fetchVersions(newEntityId)
      }
    } catch (error) {
      console.error('Error creating version:', error)
      alert('Error creating version: ' + error.message)
    }
  }

  const getVersionLineage = async (entityId, versionId) => {
    try {
      const response = await fetch(`http://localhost:8000/api/v1/versioning/entities/${entityId}/lineage/${versionId}`)
      if (response.ok) {
        const data = await response.json()
        alert('Lineage information:\n' + JSON.stringify(data, null, 2))
      }
    } catch (error) {
      console.error('Error fetching lineage:', error)
      alert('Error fetching lineage: ' + error.message)
    }
  }

  const compareVersions = async (entityId, version1, version2) => {
    try {
      const response = await fetch(`http://localhost:8000/api/v1/versioning/entities/${entityId}/compare`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          version1_id: version1,
          version2_id: version2
        })
      })

      if (response.ok) {
        const data = await response.json()
        alert('Version comparison:\n' + JSON.stringify(data, null, 2))
      }
    } catch (error) {
      console.error('Error comparing versions:', error)
      alert('Error comparing versions: ' + error.message)
    }
  }

  return (
    <div className="space-y-6">
      <div className="text-center py-6">
        <GitBranch className="w-12 h-12 mx-auto text-green-500 mb-4" />
        <h2 className="text-2xl font-bold text-gray-900 mb-2">Data Versioning</h2>
        <p className="text-gray-600 max-w-2xl mx-auto">
          Track data lineage, manage versions, and maintain audit trails for your datasets
        </p>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Create New Version */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Upload className="w-5 h-5" />
              Create New Version
            </CardTitle>
            <CardDescription>
              Create a new version of a data entity with change tracking
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <label className="block text-sm font-medium mb-2">Entity ID</label>
              <Input
                value={newEntityId}
                onChange={(e) => setNewEntityId(e.target.value)}
                placeholder="Enter entity identifier"
              />
            </div>

            <div>
              <label className="block text-sm font-medium mb-2">Change Type</label>
              <Select value={changeType} onValueChange={setChangeType}>
                <SelectTrigger>
                  <SelectValue placeholder="Select change type" />
                </SelectTrigger>
                <SelectContent>
                  {changeTypes.map((type) => (
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
              <label className="block text-sm font-medium mb-2">Change Description</label>
              <Textarea
                value={changeDescription}
                onChange={(e) => setChangeDescription(e.target.value)}
                placeholder="Describe the changes made..."
                rows={3}
              />
            </div>

            <div>
              <label className="block text-sm font-medium mb-2">Version Data (JSON or text)</label>
              <Textarea
                value={versionData}
                onChange={(e) => setVersionData(e.target.value)}
                placeholder="Enter version data as JSON or plain text..."
                rows={6}
                className="font-mono text-sm"
              />
            </div>

            <Button onClick={createNewVersion} className="w-full">
              Create Version
            </Button>
          </CardContent>
        </Card>

        {/* Entity Browser */}
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <FileText className="w-5 h-5" />
              Entity Browser
            </CardTitle>
            <CardDescription>
              Browse existing entities and their versions
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <label className="block text-sm font-medium mb-2">Select Entity</label>
              <Select value={selectedEntity} onValueChange={(value) => {
                setSelectedEntity(value)
                fetchVersions(value)
              }}>
                <SelectTrigger>
                  <SelectValue placeholder="Choose an entity to view versions" />
                </SelectTrigger>
                <SelectContent>
                  {entities.map((entity) => (
                    <SelectItem key={entity.id} value={entity.id}>
                      <div className="flex items-center gap-2">
                        <span>{entity.id}</span>
                        <Badge variant="outline" className="text-xs">
                          {entity.version_count} versions
                        </Badge>
                      </div>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {entities.length === 0 && (
              <div className="text-center py-8 text-gray-500">
                <FileText className="w-8 h-8 mx-auto mb-2 opacity-50" />
                <p>No entities found. Create your first version above.</p>
              </div>
            )}

            <Button onClick={fetchEntities} variant="outline" className="w-full">
              Refresh Entities
            </Button>
          </CardContent>
        </Card>
      </div>

      {/* Version History */}
      {selectedEntity && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Clock className="w-5 h-5" />
              Version History for {selectedEntity}
            </CardTitle>
            <CardDescription>
              Complete version history with change tracking and lineage
            </CardDescription>
          </CardHeader>
          <CardContent>
            {isLoading ? (
              <div className="text-center py-8">Loading versions...</div>
            ) : versions.length === 0 ? (
              <div className="text-center py-8 text-gray-500">
                <Clock className="w-8 h-8 mx-auto mb-2 opacity-50" />
                <p>No versions found for this entity.</p>
              </div>
            ) : (
              <div className="space-y-4">
                {versions.map((version, index) => (
                  <div key={version.id} className="border rounded-lg p-4">
                    <div className="flex justify-between items-start mb-3">
                      <div>
                        <div className="flex items-center gap-2 mb-1">
                          <Badge variant="outline">v{version.version_number}</Badge>
                          <Badge variant={
                            version.change_type === 'create' ? 'default' :
                            version.change_type === 'update' ? 'secondary' :
                            version.change_type === 'delete' ? 'destructive' :
                            'outline'
                          }>
                            {version.change_type}
                          </Badge>
                        </div>
                        <div className="text-sm text-gray-600 flex items-center gap-4">
                          <span className="flex items-center gap-1">
                            <Clock className="w-3 h-3" />
                            {new Date(version.created_at).toLocaleString()}
                          </span>
                          <span className="flex items-center gap-1">
                            <User className="w-3 h-3" />
                            {version.created_by || 'System'}
                          </span>
                        </div>
                      </div>
                      <div className="flex gap-2">
                        <Button
                          size="sm"
                          variant="outline"
                          onClick={() => getVersionLineage(selectedEntity, version.id)}
                        >
                          <Eye className="w-3 h-3 mr-1" />
                          Lineage
                        </Button>
                        {index < versions.length - 1 && (
                          <Button
                            size="sm"
                            variant="outline"
                            onClick={() => compareVersions(selectedEntity, version.id, versions[index + 1].id)}
                          >
                            Compare
                          </Button>
                        )}
                      </div>
                    </div>

                    {version.description && (
                      <div className="mb-3">
                        <div className="text-sm font-medium mb-1">Description:</div>
                        <div className="text-sm text-gray-600">{version.description}</div>
                      </div>
                    )}

                    <div className="bg-gray-50 p-3 rounded text-sm">
                      <div className="font-medium mb-1">Data Preview:</div>
                      <pre className="whitespace-pre-wrap overflow-auto max-h-32">
                        {typeof version.data === 'object' 
                          ? JSON.stringify(version.data, null, 2) 
                          : version.data || 'No data'
                        }
                      </pre>
                    </div>

                    {version.checksum && (
                      <div className="mt-2 text-xs text-gray-500">
                        Checksum: {version.checksum}
                      </div>
                    )}
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  )
}

export default DataVersioning
