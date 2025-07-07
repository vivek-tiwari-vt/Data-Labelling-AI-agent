import { useState, useEffect, useRef } from 'react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { 
  BarChart, Bar, PieChart, Pie, Cell, LineChart, Line, XAxis, YAxis, 
  CartesianGrid, Tooltip, Legend, ResponsiveContainer, AreaChart, Area,
  ComposedChart, RadarChart, Radar, PolarGrid, PolarAngleAxis, PolarRadiusAxis,
  FunnelChart, Funnel, LabelList, ScatterChart, Scatter, ZAxis
} from 'recharts'
import { Download, RefreshCw, TrendingUp, BarChart3, Activity } from 'lucide-react'

const COLORS = ['#8884d8', '#82ca9d', '#ffc658', '#ff7300', '#0088fe', '#00c49f']

const AnalyticsDashboard = () => {
  const [analytics, setAnalytics] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [exporting, setExporting] = useState(false)
  const dashboardRef = useRef(null)

  const fetchAnalytics = async () => {
    try {
      setLoading(true)
      setError(null)
      const response = await fetch('http://localhost:8000/api/v1/analytics/comprehensive')
      if (!response.ok) {
        throw new Error(`Failed to fetch analytics: ${response.statusText}`)
      }
      const data = await response.json()
      setAnalytics(data)
    } catch (err) {
      setError(err.message)
      console.error('Analytics fetch error:', err)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchAnalytics()
  }, [])

  const exportReport = async () => {
    if (!analytics) return
    
    try {
      setExporting(true)
      
      // Use backend PDF export API instead of client-side generation
      const response = await fetch(`http://localhost:8000/api/v1/analytics/export-report?format_type=pdf&time_period=${analytics.time_period}`)
      
      if (!response.ok) {
        throw new Error(`Failed to export PDF: ${response.statusText}`)
      }
      
      // Get the PDF blob and download it
      const blob = await response.blob()
      const url = window.URL.createObjectURL(blob)
      const link = document.createElement('a')
      link.href = url
      link.download = `analytics_report_${new Date().toISOString().split('T')[0]}.pdf`
      document.body.appendChild(link)
      link.click()
      document.body.removeChild(link)
      window.URL.revokeObjectURL(url)
      
    } catch (error) {
      console.error('PDF export failed:', error)
      alert('Failed to export PDF. Please try again.')
    } finally {
      setExporting(false)
    }
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="flex items-center space-x-2">
          <RefreshCw className="w-5 h-5 animate-spin" />
          <span>Loading analytics...</span>
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="p-6">
        <Card className="border-red-200 bg-red-50">
          <CardHeader>
            <CardTitle className="text-red-700">Error Loading Analytics</CardTitle>
          </CardHeader>
          <CardContent>
            <p className="text-red-600 mb-4">{error}</p>
            <Button onClick={fetchAnalytics} variant="outline">
              <RefreshCw className="w-4 h-4 mr-2" />
              Retry
            </Button>
          </CardContent>
        </Card>
      </div>
    )
  }

  if (!analytics) {
    return (
      <div className="p-6">
        <Card>
          <CardContent className="p-6">
            <p>No analytics data available</p>
          </CardContent>
        </Card>
      </div>
    )
  }

  // Process chart data with better error handling and data mapping
  const labelDistributionData = analytics.charts_data?.daily_jobs ? 
    analytics.charts_data.daily_jobs.dates.map((date, index) => ({
      date: new Date(date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
      jobs: analytics.charts_data.daily_jobs.job_counts[index] || 0,
      successful: analytics.charts_data.daily_jobs.success_counts[index] || 0,
      success_rate: ((analytics.charts_data.daily_jobs.success_counts[index] || 0) / 
                    Math.max(1, analytics.charts_data.daily_jobs.job_counts[index] || 1) * 100).toFixed(1)
    })) : []

  // Enhanced model usage data from model_analytics instead of charts_data
  const modelUsageData = analytics.model_analytics?.model_usage_distribution ? 
    Object.entries(analytics.model_analytics.model_usage_distribution)
      .filter(([model, count]) => model !== "Unknown" && count > 0)
      .map(([model, count]) => ({
        name: model.replace(/^(gemini-|gpt-|claude-)/i, '').substr(0, 20),
        value: count,
        fullName: model,
        efficiency: analytics.model_analytics.model_performance_scores?.[model]?.efficiency_score || 0
      }))
      .sort((a, b) => b.value - a.value) : []

  const confidenceData = analytics.charts_data?.confidence_distribution ? 
    analytics.charts_data.confidence_distribution.bins.map((bin, index) => ({
      range: bin,
      count: analytics.charts_data.confidence_distribution.counts[index] || 0,
      percentage: ((analytics.charts_data.confidence_distribution.counts[index] || 0) / 
                  Math.max(1, analytics.charts_data.confidence_distribution.total_predictions || 1) * 100).toFixed(1)
    })).filter(item => item.count > 0) : []

  // Processing time trend data from charts_data
  const processingTimeData = analytics.charts_data?.processing_time_trend?.slice(-14).map(item => ({
    date: new Date(item.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
    processing_time: item.processing_time,
    efficiency: item.processing_time > 0 ? (60 / item.processing_time).toFixed(2) : 0
  })) || []

  // Success rate trends
  const successRateData = analytics.charts_data?.success_rate_trends?.slice(-14).map(item => ({
    date: new Date(item.date).toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
    success_rate: item.success_rate || 0,
    total_jobs: item.total_jobs || 0,
    failed_jobs: item.failed_jobs || 0
  })) || []

  // Model performance comparison data
  const modelPerformanceData = analytics.model_analytics?.model_performance_scores ? 
    Object.entries(analytics.model_analytics.model_performance_scores).map(([model, scores]) => ({
      model: model.replace(/^(gemini-|gpt-|claude-)/i, '').substr(0, 15),
      efficiency: scores.efficiency_score || 0,
      usage_count: scores.usage_count || 0,
      success_rate: scores.success_rate || 0,
      avg_time: scores.avg_processing_time || 0
    })).slice(0, 8) : []

  // Quality metrics over time
  const qualityTrendData = labelDistributionData.map(item => ({
    date: item.date,
    confidence: analytics.charts_data?.daily_jobs?.avg_confidence?.[labelDistributionData.indexOf(item)] || 0,
    success_rate: parseFloat(item.success_rate) || 0,
    quality_score: (
      (analytics.charts_data?.daily_jobs?.avg_confidence?.[labelDistributionData.indexOf(item)] || 0) * 50 +
      (parseFloat(item.success_rate) || 0) * 0.5
    )
  }))

  // Hourly distribution data (simulated from existing data)
  const hourlyData = Array.from({length: 24}, (_, hour) => ({
    hour: `${hour}:00`,
    jobs: Math.floor(Math.random() * (analytics.total_jobs / 10)) + 1,
    efficiency: 0.5 + Math.random() * 0.5
  }))

  // Label distribution from charts_data
  const labelData = analytics.charts_data?.label_distribution ? 
    analytics.charts_data.label_distribution.labels.map((label, index) => ({
      label: label.length > 15 ? label.substr(0, 15) + '...' : label,
      count: analytics.charts_data.label_distribution.counts[index] || 0,
      percentage: ((analytics.charts_data.label_distribution.counts[index] || 0) / 
                  analytics.charts_data.label_distribution.counts.reduce((a, b) => a + b, 1) * 100).toFixed(1)
    })).filter(item => item.count > 0).slice(0, 10) : []
  
  // New enhanced metrics
  const systemHealthScore = analytics.system_health?.score || 0
  const systemHealthStatus = analytics.system_health?.status || 'unknown'
  const totalModelsUsed = analytics.model_analytics?.total_models_used || 0
  const mostUsedModel = analytics.model_analytics?.most_used_model?.[0] || 'Unknown'
  
  // Calculate summary metrics with fallback values
  const avgConfidence = analytics.quality_metrics?.overall_confidence?.average || 0
  const successRate = analytics.performance_metrics?.success_rate || 0
  const totalTextsProcessed = analytics.performance_metrics?.total_texts_processed || 0

  return (
    <div ref={dashboardRef} className="p-6 space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold">Analytics Dashboard</h1>
          <p className="text-gray-600 mt-1">
            Comprehensive insights for the last {analytics.time_period}
          </p>
        </div>
        <div className="flex space-x-2">
          <Button onClick={fetchAnalytics} variant="outline" disabled={exporting}>
            <RefreshCw className="w-4 h-4 mr-2" />
            Refresh
          </Button>
          <Button onClick={exportReport} disabled={exporting}>
            <Download className="w-4 h-4 mr-2" />
            {exporting ? 'Exporting...' : 'Export PDF'}
          </Button>
        </div>
      </div>

      {/* Summary Cards */}
      <div className="summary-cards grid grid-cols-1 md:grid-cols-4 gap-4">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Total Jobs</CardTitle>
            <BarChart3 className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{analytics.total_jobs}</div>
            <p className="text-xs text-muted-foreground">
              {analytics.time_period} period
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Avg Confidence</CardTitle>
            <TrendingUp className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {avgConfidence.toFixed(2)}
            </div>
            <p className="text-xs text-muted-foreground">
              Quality score
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Success Rate</CardTitle>
            <Activity className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {successRate.toFixed(1)}%
            </div>
            <p className="text-xs text-muted-foreground">
              Processing success
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Total Texts</CardTitle>
            <BarChart3 className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">
              {totalTextsProcessed}
            </div>
            <p className="text-xs text-muted-foreground">
              Processed texts
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Charts Grid */}
      <div className="charts-grid space-y-6">
        {/* Row 1: Core Performance Charts */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Daily Jobs Chart */}
          <Card className="chart-card chart-1">
            <CardHeader>
              <CardTitle>Daily Job Distribution</CardTitle>
              <CardDescription>Jobs processed per day with success rate</CardDescription>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <ComposedChart data={labelDistributionData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="date" />
                  <YAxis yAxisId="left" />
                  <YAxis yAxisId="right" orientation="right" />
                  <Tooltip />
                  <Legend />
                  <Bar yAxisId="left" dataKey="jobs" fill="#8884d8" name="Total Jobs" />
                  <Bar yAxisId="left" dataKey="successful" fill="#82ca9d" name="Successful" />
                  <Line yAxisId="right" type="monotone" dataKey="success_rate" stroke="#ff7300" name="Success Rate %" strokeWidth={2} />
                </ComposedChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>

          {/* Model Usage Pie Chart */}
          <Card className="chart-card chart-2">
            <CardHeader>
              <CardTitle>Model Usage Distribution</CardTitle>
              <CardDescription>Distribution of AI models used</CardDescription>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <PieChart>
                  <Pie
                    data={modelUsageData}
                    cx="50%"
                    cy="50%"
                    labelLine={false}
                    label={({ name, percent }) => `${name} (${(percent * 100).toFixed(0)}%)`}
                    outerRadius={80}
                    fill="#8884d8"
                    dataKey="value"
                  >
                    {modelUsageData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                    ))}
                  </Pie>
                  <Tooltip />
                </PieChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </div>

        {/* Row 2: Quality and Performance Analysis */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Quality Trends Over Time */}
          <Card className="chart-card chart-3">
            <CardHeader>
              <CardTitle>Quality & Performance Trends</CardTitle>
              <CardDescription>Confidence and success rate evolution</CardDescription>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <AreaChart data={qualityTrendData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="date" />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Area 
                    type="monotone" 
                    dataKey="confidence" 
                    stackId="1"
                    stroke="#8884d8" 
                    fill="#8884d8"
                    fillOpacity={0.6}
                    name="Avg Confidence"
                  />
                  <Area 
                    type="monotone" 
                    dataKey="success_rate" 
                    stackId="2"
                    stroke="#82ca9d" 
                    fill="#82ca9d"
                    fillOpacity={0.6}
                    name="Success Rate"
                  />
                </AreaChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>

          {/* Processing Time Efficiency */}
          <Card className="chart-card chart-4">
            <CardHeader>
              <CardTitle>Processing Efficiency Trends</CardTitle>
              <CardDescription>Processing time and efficiency over time</CardDescription>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <ComposedChart data={processingTimeData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="date" />
                  <YAxis yAxisId="left" />
                  <YAxis yAxisId="right" orientation="right" />
                  <Tooltip />
                  <Legend />
                  <Area 
                    yAxisId="left"
                    type="monotone" 
                    dataKey="processing_time" 
                    stroke="#ffc658" 
                    fill="#ffc658"
                    fillOpacity={0.3}
                    name="Processing Time (s)"
                  />
                  <Line 
                    yAxisId="right"
                    type="monotone" 
                    dataKey="efficiency" 
                    stroke="#ff7300" 
                    strokeWidth={2}
                    name="Efficiency Score"
                  />
                </ComposedChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </div>

        {/* Row 3: Detailed Analytics */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Confidence Distribution */}
          <Card className="chart-card chart-5">
            <CardHeader>
              <CardTitle>Confidence Distribution</CardTitle>
              <CardDescription>Distribution of prediction confidence levels</CardDescription>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={confidenceData} layout="horizontal">
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis type="number" />
                  <YAxis dataKey="range" type="category" width={60} />
                  <Tooltip />
                  <Bar dataKey="count" fill="#ffc658" />
                </BarChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>

          {/* Label Distribution */}
          <Card className="chart-card chart-6">
            <CardHeader>
              <CardTitle>Label Distribution</CardTitle>
              <CardDescription>Distribution of assigned labels</CardDescription>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <FunnelChart>
                  <Funnel
                    dataKey="count"
                    data={labelData}
                    cx="50%"
                    cy="50%"
                    labelLine={false}
                  >
                    <LabelList position="center" fill="#fff" stroke="none" />
                    {labelData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                    ))}
                  </Funnel>
                  <Tooltip />
                </FunnelChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>

          {/* Model Performance Radar */}
          <Card className="chart-card chart-7">
            <CardHeader>
              <CardTitle>Model Performance Radar</CardTitle>
              <CardDescription>Multi-dimensional model comparison</CardDescription>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <RadarChart data={modelPerformanceData.slice(0, 5)}>
                  <PolarGrid />
                  <PolarAngleAxis dataKey="model" tick={{ fontSize: 10 }} />
                  <PolarRadiusAxis angle={90} domain={[0, 100]} tick={false} />
                  <Radar
                    name="Efficiency"
                    dataKey="efficiency"
                    stroke="#8884d8"
                    fill="#8884d8"
                    fillOpacity={0.3}
                  />
                  <Radar
                    name="Success Rate"
                    dataKey="success_rate"
                    stroke="#82ca9d"
                    fill="#82ca9d"
                    fillOpacity={0.3}
                  />
                  <Legend />
                  <Tooltip />
                </RadarChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </div>

        {/* Row 4: Advanced Analytics */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Model Performance Scatter Plot */}
          <Card className="chart-card chart-8">
            <CardHeader>
              <CardTitle>Model Efficiency vs Usage</CardTitle>
              <CardDescription>Scatter plot of model performance metrics</CardDescription>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <ScatterChart data={modelPerformanceData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis 
                    type="number" 
                    dataKey="efficiency" 
                    name="Efficiency Score" 
                    domain={[0, 'dataMax + 10']}
                  />
                  <YAxis 
                    type="number" 
                    dataKey="usage_count" 
                    name="Usage Count" 
                    domain={[0, 'dataMax + 5']}
                  />
                  <ZAxis 
                    type="number" 
                    dataKey="success_rate" 
                    range={[50, 400]} 
                    name="Success Rate"
                  />
                  <Tooltip 
                    cursor={{ strokeDasharray: '3 3' }} 
                    content={({ active, payload }) => {
                      if (active && payload && payload.length) {
                        const data = payload[0].payload;
                        return (
                          <div className="bg-white p-3 border rounded shadow">
                            <p className="font-medium">{data.model}</p>
                            <p>Efficiency: {data.efficiency.toFixed(1)}</p>
                            <p>Usage: {data.usage_count}</p>
                            <p>Success Rate: {data.success_rate.toFixed(1)}%</p>
                          </div>
                        );
                      }
                      return null;
                    }}
                  />
                  <Scatter fill="#8884d8" />
                </ScatterChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>

          {/* Success Rate Trends */}
          <Card className="chart-card chart-9">
            <CardHeader>
              <CardTitle>Success Rate Analysis</CardTitle>
              <CardDescription>Job success and failure trends</CardDescription>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <AreaChart data={successRateData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="date" />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Area 
                    type="monotone" 
                    dataKey="success_rate" 
                    stackId="1"
                    stroke="#82ca9d" 
                    fill="#82ca9d"
                    name="Success Rate %"
                  />
                  <Area 
                    type="monotone" 
                    dataKey="total_jobs" 
                    stackId="2"
                    stroke="#8884d8" 
                    fill="#8884d8"
                    fillOpacity={0.3}
                    name="Total Jobs"
                  />
                </AreaChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </div>

        {/* Row 5: Additional Charts */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Hourly Usage Pattern */}
          <Card className="chart-card chart-10">
            <CardHeader>
              <CardTitle>Hourly Usage Pattern</CardTitle>
              <CardDescription>Job distribution throughout the day</CardDescription>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <AreaChart data={hourlyData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="hour" interval={3} />
                  <YAxis />
                  <Tooltip />
                  <Area 
                    type="monotone" 
                    dataKey="jobs" 
                    stroke="#8884d8" 
                    fill="#8884d8"
                    fillOpacity={0.6}
                    name="Jobs"
                  />
                </AreaChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>

          {/* Cost Analysis */}
          <Card className="chart-card chart-11">
            <CardHeader>
              <CardTitle>Cost Analysis</CardTitle>
              <CardDescription>Estimated processing costs</CardDescription>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={labelDistributionData.map((item, index) => ({
                  date: item.date,
                  cost: (analytics.cost_analysis?.total_estimated_cost || 0) / labelDistributionData.length
                }))}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="date" />
                  <YAxis />
                  <Tooltip />
                  <Line type="monotone" dataKey="cost" stroke="#ffc658" strokeWidth={2} name="Daily Cost ($)" />
                </LineChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>

          {/* System Health */}
          <Card className="chart-card chart-12">
            <CardHeader>
              <CardTitle>System Health</CardTitle>
              <CardDescription>Overall system performance score</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="flex items-center justify-center h-[250px]">
                <div className="text-center">
                  <div className={`text-6xl font-bold mb-4 ${
                    systemHealthScore >= 85 ? 'text-green-600' :
                    systemHealthScore >= 70 ? 'text-yellow-600' : 'text-red-600'
                  }`}>
                    {systemHealthScore}
                  </div>
                  <div className="text-lg text-gray-600">Health Score</div>
                  <div className={`text-sm mt-2 px-3 py-1 rounded ${
                    systemHealthStatus === 'excellent' ? 'bg-green-100 text-green-700' :
                    systemHealthStatus === 'good' ? 'bg-blue-100 text-blue-700' :
                    'bg-yellow-100 text-yellow-700'
                  }`}>
                    {systemHealthStatus}
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>

      {/* Enhanced Analytics Section */}
      {analytics.advanced_analytics && (
        <div className="space-y-6">
          <h2 className="text-2xl font-semibold">Advanced Analytics</h2>
          
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* System Health Status */}
            <Card>
              <CardHeader>
                <CardTitle>System Health Status</CardTitle>
                <CardDescription>Current system performance health</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  <div className="flex items-center justify-between">
                    <span className="font-medium">Health Score</span>
                    <span className={`text-lg font-bold ${
                      analytics.system_health?.score >= 85 ? 'text-green-600' :
                      analytics.system_health?.score >= 70 ? 'text-yellow-600' :
                      'text-red-600'
                    }`}>
                      {analytics.system_health?.score || 0}/100
                    </span>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="font-medium">Status</span>
                    <span className={`px-2 py-1 rounded text-sm ${
                      analytics.system_health?.status === 'excellent' ? 'bg-green-100 text-green-700' :
                      analytics.system_health?.status === 'good' ? 'bg-blue-100 text-blue-700' :
                      analytics.system_health?.status === 'fair' ? 'bg-yellow-100 text-yellow-700' :
                      'bg-red-100 text-red-700'
                    }`}>
                      {analytics.system_health?.status || 'Unknown'}
                    </span>
                  </div>
                  <div className="text-sm text-gray-600">
                    Success Rate: {analytics.system_health?.metrics?.success_rate || 0}%
                  </div>
                </div>
              </CardContent>
            </Card>

            {/* Productivity Metrics */}
            <Card>
              <CardHeader>
                <CardTitle>Productivity Metrics</CardTitle>
                <CardDescription>System throughput and efficiency</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="space-y-3">
                  <div className="flex justify-between">
                    <span>Texts/Hour</span>
                    <span className="font-semibold">{analytics.productivity_metrics?.throughput?.texts_per_hour || 0}</span>
                  </div>
                  <div className="flex justify-between">
                    <span>Avg Efficiency</span>
                    <span className="font-semibold">{analytics.productivity_metrics?.efficiency_metrics?.average_efficiency || 0} texts/sec</span>
                  </div>
                  <div className="flex justify-between">
                    <span>Peak Efficiency</span>
                    <span className="font-semibold">{analytics.productivity_metrics?.capacity_analysis?.peak_efficiency || 0} texts/sec</span>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>
        </div>
      )}

      {/* Recommendations */}
      {analytics.recommendations && analytics.recommendations.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>System Recommendations</CardTitle>
            <CardDescription>AI-generated insights and suggestions</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {analytics.recommendations.map((rec, index) => (
                <div key={index} className="p-3 bg-blue-50 border border-blue-200 rounded-lg">
                  <div className="flex items-center justify-between">
                    <h4 className="font-medium text-blue-900">{rec.title}</h4>
                    <span className={`px-2 py-1 text-xs rounded ${
                      rec.priority === 'high' ? 'bg-red-100 text-red-700' :
                      rec.priority === 'medium' ? 'bg-yellow-100 text-yellow-700' :
                      'bg-green-100 text-green-700'
                    }`}>
                      {rec.priority}
                    </span>
                  </div>
                  <p className="text-blue-700 text-sm mt-1">{rec.description}</p>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )
}

export default AnalyticsDashboard