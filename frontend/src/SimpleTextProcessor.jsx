import React, { useState } from 'react'
import { Button } from '@/components/ui/button.jsx'
import { Textarea } from '@/components/ui/textarea.jsx'
import { Input } from '@/components/ui/input.jsx'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select.jsx'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card.jsx'
import { Progress } from '@/components/ui/progress.jsx'
import { Badge } from '@/components/ui/badge.jsx'
import { Upload, Bot, Brain } from 'lucide-react'

const SimpleTextProcessor = () => {
  const [selectedFile, setSelectedFile] = useState(null)
  const [availableLabels, setAvailableLabels] = useState('')
  const [instructions, setInstructions] = useState('Analyze each text and assign the most appropriate label from the provided labels, also make sure that you analyze all the text properly.')
  const [motherAiModel, setMotherAiModel] = useState('')
  const [childAiModel, setChildAiModel] = useState('')
  const [jobId, setJobId] = useState(null)
  const [jobStatus, setJobStatus] = useState(null)
  const [progress, setProgress] = useState(0)
  const [isProcessing, setIsProcessing] = useState(false)

  // Available model options
  const openRouterModels = [
    { value: 'deepseek/deepseek-r1-0528-qwen3-8b:free', label: 'DeepSeek R1 Qwen3 8B' },
    { value: 'mistralai/mistral-small-3.2-24b-instruct:free', label: 'Mistral Small 3.2 24B' },
    { value: 'moonshotai/kimi-dev-72b:free', label: 'Moonshot Kimi Dev 72B' },
    { value: 'meta-llama/llama-4-scout:free', label: 'Meta Llama 4 Scout' }
  ]

  const geminiModels = [
    { value: 'gemini-2.0-flash', label: 'Gemini 2.0 Flash' }
  ]

  const allModels = [
    ...openRouterModels.map(model => ({ ...model, provider: 'openrouter' })),
    ...geminiModels.map(model => ({ ...model, provider: 'gemini' }))
  ]

  const handleFileChange = (event) => {
    const file = event.target.files[0]
    setSelectedFile(file)
  }

  const getModelLabel = (modelValue) => {
    const model = allModels.find(m => m.value === modelValue)
    return model ? model.label : modelValue
  }

  const getModelProvider = (modelValue) => {
    const model = allModels.find(m => m.value === modelValue)
    return model ? model.provider : 'unknown'
  }

  return (
    <div className="max-w-4xl mx-auto p-6 space-y-6">
      <div className="text-center space-y-2">
        <h1 className="text-3xl font-bold">Multi-Agent Text Processing System</h1>
        <p className="text-gray-600">Intelligent AI-powered text analysis and classification</p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Upload className="h-5 w-5" />
            Text Processing
          </CardTitle>
          <CardDescription>
            Upload a JSON file containing texts and configure AI models for intelligent classification
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-6">
          {/* Model Selection Section */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 p-4 bg-gray-50 rounded-lg">
            <div className="space-y-2">
              <label className="block text-sm font-medium flex items-center gap-2">
                <Brain className="h-4 w-4" />
                Mother AI Model
              </label>
              <Select value={motherAiModel} onValueChange={setMotherAiModel}>
                <SelectTrigger>
                  <SelectValue placeholder="Select Mother AI model" />
                </SelectTrigger>
                <SelectContent>
                  {allModels.map((model) => (
                    <SelectItem key={model.value} value={model.value}>
                      <div className="flex items-center gap-2">
                        <Badge variant={model.provider === 'gemini' ? 'default' : 'secondary'} className="text-xs">
                          {model.provider === 'gemini' ? 'Gemini' : 'OpenRouter'}
                        </Badge>
                        {model.label}
                      </div>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              {motherAiModel && (
                <p className="text-xs text-gray-600">
                  Selected: {getModelLabel(motherAiModel)} ({getModelProvider(motherAiModel)})
                </p>
              )}
            </div>

            <div className="space-y-2">
              <label className="block text-sm font-medium flex items-center gap-2">
                <Bot className="h-4 w-4" />
                Child AI Model
              </label>
              <Select value={childAiModel} onValueChange={setChildAiModel}>
                <SelectTrigger>
                  <SelectValue placeholder="Select Child AI model" />
                </SelectTrigger>
                <SelectContent>
                  {allModels.map((model) => (
                    <SelectItem key={model.value} value={model.value}>
                      <div className="flex items-center gap-2">
                        <Badge variant={model.provider === 'gemini' ? 'default' : 'secondary'} className="text-xs">
                          {model.provider === 'gemini' ? 'Gemini' : 'OpenRouter'}
                        </Badge>
                        {model.label}
                      </div>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              {childAiModel && (
                <p className="text-xs text-gray-600">
                  Selected: {getModelLabel(childAiModel)} ({getModelProvider(childAiModel)})
                </p>
              )}
            </div>
          </div>

          {/* File Upload Section */}
          <div>
            <label className="block text-sm font-medium mb-2">Select File (JSON)</label>
            <Input
              type="file"
              accept=".json,.csv,.xml"
              onChange={handleFileChange}
              className="cursor-pointer"
            />
            {selectedFile && (
              <p className="text-sm text-gray-600 mt-1">
                Selected: {selectedFile.name}
              </p>
            )}
            <p className="text-xs text-gray-500 mt-1">
              Supported formats: JSON (test_texts array), CSV (text column), XML (text elements)
            </p>
          </div>
          
          {/* Labels Section */}
          <div>
            <label className="block text-sm font-medium mb-2">Available Labels (comma-separated)</label>
            <Input
              placeholder="e.g., product_review, news, science_research, personal_social, complaint, question_help"
              value={availableLabels}
              onChange={(e) => setAvailableLabels(e.target.value)}
            />
            <p className="text-xs text-gray-500 mt-1">
              Separate labels with commas. AI will choose ONE label per text using intelligent analysis.
            </p>
          </div>
          
          {/* Instructions Section */}
          <div>
            <label className="block text-sm font-medium mb-2">Classification Instructions</label>
            <Textarea
              value={instructions}
              onChange={(e) => setInstructions(e.target.value)}
              rows={3}
            />
            <p className="text-xs text-gray-500 mt-1">
              Provide clear instructions to guide the AI models in making accurate classification decisions.
            </p>
          </div>
          
          {/* Action Buttons */}
          <div className="flex gap-2">
            <Button 
              disabled={!selectedFile || !availableLabels.trim() || !motherAiModel || !childAiModel || isProcessing}
              className="flex items-center gap-2"
            >
              <Upload className="h-4 w-4" />
              Start Text Processing
            </Button>
          </div>
          
          {/* Labels Preview */}
          {availableLabels && (
            <div className="bg-blue-50 p-3 rounded-lg">
              <p className="text-sm font-medium text-blue-800">Labels to use:</p>
              <div className="flex flex-wrap gap-1 mt-1">
                {availableLabels.split(',').map((label, index) => (
                  <Badge key={index} variant="outline" className="text-blue-700 border-blue-300">
                    {label.trim()}
                  </Badge>
                ))}
              </div>
            </div>
          )}

          {/* Model Preview */}
          {(motherAiModel || childAiModel) && (
            <div className="bg-green-50 p-3 rounded-lg">
              <p className="text-sm font-medium text-green-800">Selected Models:</p>
              <div className="mt-1 space-y-1">
                {motherAiModel && (
                  <div className="flex items-center gap-2 text-sm">
                    <Brain className="h-3 w-3" />
                    <span className="font-medium">Mother AI:</span>
                    <Badge variant="outline">{getModelLabel(motherAiModel)}</Badge>
                  </div>
                )}
                {childAiModel && (
                  <div className="flex items-center gap-2 text-sm">
                    <Bot className="h-3 w-3" />
                    <span className="font-medium">Child AI:</span>
                    <Badge variant="outline">{getModelLabel(childAiModel)}</Badge>
                  </div>
                )}
              </div>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Progress Section */}
      {(isProcessing || progress > 0) && (
        <Card>
          <CardHeader>
            <CardTitle>Processing Status</CardTitle>
            <CardDescription>Job ID: {jobId}</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <div className="flex justify-between text-sm">
                <span>Progress</span>
                <span>{progress.toFixed(0)}%</span>
              </div>
              <Progress value={progress} className="w-full" />
            </div>
            
            {jobStatus && (
              <div className="flex items-center gap-2">
                <Badge variant={
                  jobStatus === 'completed' ? 'default' : 
                  jobStatus === 'failed' ? 'destructive' : 
                  'secondary'
                }>
                  {jobStatus}
                </Badge>
              </div>
            )}
          </CardContent>
        </Card>
      )}
    </div>
  )
}

export default SimpleTextProcessor
