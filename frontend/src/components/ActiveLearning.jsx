import { useState, useEffect } from 'react'
import { Button } from '@/components/ui/button.jsx'
import { Input } from '@/components/ui/input.jsx'
import { Textarea } from '@/components/ui/textarea.jsx'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select.jsx'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card.jsx'
import { Badge } from '@/components/ui/badge.jsx'
import { Progress } from '@/components/ui/progress.jsx'
import { Brain, Target, TrendingUp, AlertCircle, CheckCircle, Upload } from 'lucide-react'

const ActiveLearning = () => {
  const [jobId, setJobId] = useState('')
  const [analysisResults, setAnalysisResults] = useState(null)
  const [uncertainSamples, setUncertainSamples] = useState([])
  const [isAnalyzing, setIsAnalyzing] = useState(false)
  const [samplingStrategy, setSamplingStrategy] = useState('uncertainty')
  const [sampleCount, setSampleCount] = useState(10)
  const [confidenceThreshold, setConfidenceThreshold] = useState(0.7)

  const strategies = [
    { value: 'uncertainty', label: 'Uncertainty Sampling', description: 'Select samples with highest prediction uncertainty' },
    { value: 'margin', label: 'Margin Sampling', description: 'Select samples with smallest margin between top predictions' },
    { value: 'entropy', label: 'Entropy Sampling', description: 'Select samples with highest prediction entropy' },
    { value: 'diversity', label: 'Diversity Sampling', description: 'Select diverse samples to improve model coverage' }
  ]

  const analyzeJob = async () => {
    if (!jobId.trim()) {
      alert('Please enter a job ID')
      return
    }

    setIsAnalyzing(true)
    try {
      const response = await fetch(`http://localhost:8000/api/v1/active-learning/analyze-job/${jobId}?strategy=${samplingStrategy}&sample_count=${sampleCount}&confidence_threshold=${confidenceThreshold}`, {
        method: 'POST'
      })

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`)
      }

      const data = await response.json()
      setAnalysisResults(data)
      setUncertainSamples(data.uncertain_samples || [])
    } catch (error) {
      console.error('Error analyzing job:', error)
      alert('Error analyzing job: ' + error.message)
    } finally {
      setIsAnalyzing(false)
    }
  }

  const getSamplesForLabeling = async () => {
    try {
      const response = await fetch(`http://localhost:8000/api/v1/active-learning/samples/${jobId}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          strategy: samplingStrategy,
          count: sampleCount
        })
      })

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`)
      }

      const data = await response.json()
      setUncertainSamples(data.samples || [])
    } catch (error) {
      console.error('Error getting samples:', error)
      alert('Error getting samples: ' + error.message)
    }
  }

  const updateSampleLabel = async (sampleId, newLabel) => {
    try {
      const response = await fetch(`http://localhost:8000/api/v1/active-learning/feedback`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          job_id: jobId,
          sample_id: sampleId,
          label: newLabel,
          feedback_type: 'correction'
        })
      })

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`)
      }

      // Update local state
      setUncertainSamples(prev => prev.map(sample => 
        sample.id === sampleId ? { ...sample, label: newLabel, status: 'corrected' } : sample
      ))
    } catch (error) {
      console.error('Error updating sample:', error)
      alert('Error updating sample: ' + error.message)
    }
  }

  return (
    <div className="space-y-6">
      <div className="text-center py-6">
        <Brain className="w-12 h-12 mx-auto text-blue-500 mb-4" />
        <h2 className="text-2xl font-bold text-gray-900 mb-2">Active Learning</h2>
        <p className="text-gray-600 max-w-2xl mx-auto">
          Intelligently identify uncertain predictions and improve model performance through targeted labeling
        </p>
      </div>

      {/* Analysis Configuration */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Target className="w-5 h-5" />
            Analysis Configuration
          </CardTitle>
          <CardDescription>
            Configure active learning parameters for your job analysis
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium mb-2">Job ID</label>
              <Input
                value={jobId}
                onChange={(e) => setJobId(e.target.value)}
                placeholder="Enter job ID to analyze"
                disabled={isAnalyzing}
              />
            </div>
            <div>
              <label className="block text-sm font-medium mb-2">Sample Count</label>
              <Input
                type="number"
                value={sampleCount}
                onChange={(e) => setSampleCount(parseInt(e.target.value) || 10)}
                min="1"
                max="100"
                disabled={isAnalyzing}
              />
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium mb-2">Sampling Strategy</label>
              <Select value={samplingStrategy} onValueChange={setSamplingStrategy} disabled={isAnalyzing}>
                <SelectTrigger>
                  <SelectValue placeholder="Select strategy" />
                </SelectTrigger>
                <SelectContent>
                  {strategies.map((strategy) => (
                    <SelectItem key={strategy.value} value={strategy.value}>
                      <div>
                        <div className="font-medium">{strategy.label}</div>
                        <div className="text-xs text-gray-500">{strategy.description}</div>
                      </div>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div>
              <label className="block text-sm font-medium mb-2">Confidence Threshold</label>
              <Input
                type="number"
                value={confidenceThreshold}
                onChange={(e) => setConfidenceThreshold(parseFloat(e.target.value) || 0.7)}
                min="0"
                max="1"
                step="0.1"
                disabled={isAnalyzing}
              />
            </div>
          </div>

          <div className="flex gap-2">
            <Button onClick={analyzeJob} disabled={isAnalyzing || !jobId.trim()}>
              {isAnalyzing ? 'Analyzing...' : 'Analyze Job'}
            </Button>
            <Button variant="outline" onClick={getSamplesForLabeling} disabled={!jobId.trim()}>
              Get Samples for Labeling
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Analysis Results */}
      {analysisResults && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <TrendingUp className="w-5 h-5" />
              Analysis Results
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="text-center p-4 bg-blue-50 rounded-lg">
                <div className="text-2xl font-bold text-blue-600">
                  {analysisResults.total_samples || 0}
                </div>
                <div className="text-sm text-gray-600">Total Samples</div>
              </div>
              <div className="text-center p-4 bg-yellow-50 rounded-lg">
                <div className="text-2xl font-bold text-yellow-600">
                  {analysisResults.uncertain_count || 0}
                </div>
                <div className="text-sm text-gray-600">Uncertain Samples</div>
              </div>
              <div className="text-center p-4 bg-green-50 rounded-lg">
                <div className="text-2xl font-bold text-green-600">
                  {analysisResults.confidence_score ? (analysisResults.confidence_score * 100).toFixed(1) : 0}%
                </div>
                <div className="text-sm text-gray-600">Avg Confidence</div>
              </div>
            </div>

            {analysisResults.recommendations && (
              <div className="bg-gray-50 p-4 rounded-lg">
                <h4 className="font-medium mb-2">Recommendations:</h4>
                <ul className="text-sm text-gray-600 space-y-1">
                  {analysisResults.recommendations.map((rec, index) => (
                    <li key={index} className="flex items-start gap-2">
                      <AlertCircle className="w-4 h-4 mt-0.5 text-blue-500" />
                      {rec}
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* Uncertain Samples */}
      {uncertainSamples.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <AlertCircle className="w-5 h-5" />
              Uncertain Samples for Review
            </CardTitle>
            <CardDescription>
              Review and correct these uncertain predictions to improve model performance
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {uncertainSamples.map((sample, index) => (
                <div key={sample.id || index} className="border rounded-lg p-4">
                  <div className="flex justify-between items-start mb-2">
                    <div className="flex-1">
                      <div className="text-sm font-medium">Sample #{sample.id || index + 1}</div>
                      <div className="text-sm text-gray-600">
                        Confidence: {(sample.confidence * 100).toFixed(1)}%
                      </div>
                    </div>
                    <Badge variant={sample.confidence < 0.5 ? 'destructive' : sample.confidence < 0.7 ? 'warning' : 'secondary'}>
                      {sample.confidence < 0.5 ? 'Low' : sample.confidence < 0.7 ? 'Medium' : 'High'} Confidence
                    </Badge>
                  </div>
                  
                  <div className="mb-3 p-3 bg-gray-50 rounded">
                    <div className="text-sm font-medium mb-1">Text:</div>
                    <div className="text-sm">{sample.text}</div>
                  </div>

                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                      <div className="text-sm font-medium mb-1">Predicted Label:</div>
                      <Badge variant="outline">{sample.predicted_label}</Badge>
                    </div>
                    <div>
                      <div className="text-sm font-medium mb-1">Correct Label:</div>
                      <div className="flex gap-2">
                        <Input
                          placeholder="Enter correct label"
                          className="flex-1"
                          onKeyPress={(e) => {
                            if (e.key === 'Enter' && e.target.value.trim()) {
                              updateSampleLabel(sample.id, e.target.value.trim())
                              e.target.value = ''
                            }
                          }}
                        />
                        <Button
                          size="sm"
                          variant="outline"
                          onClick={() => {
                            const input = event.target.previousElementSibling
                            if (input.value.trim()) {
                              updateSampleLabel(sample.id, input.value.trim())
                              input.value = ''
                            }
                          }}
                        >
                          Update
                        </Button>
                      </div>
                    </div>
                  </div>

                  {sample.status === 'corrected' && (
                    <div className="mt-2 flex items-center gap-2 text-green-600">
                      <CheckCircle className="w-4 h-4" />
                      <span className="text-sm">Label updated successfully</span>
                    </div>
                  )}
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )
}

export default ActiveLearning
